package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─── Client registry ──────────────────────────────────────────────────────────

type Client struct {
	id   string
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string]*Client)}
}

func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	h.clients[c.id] = c
	h.mu.Unlock()
	log.Printf("client connected: %s (total: %d)", c.id[:8], len(h.clients))
}

func (h *Hub) Unregister(id string) {
	h.mu.Lock()
	delete(h.clients, id)
	h.mu.Unlock()
	log.Printf("client disconnected: %s (total: %d)", id[:8], len(h.clients))
}

// Broadcast sends a message to every connected client.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.clients {
		select {
		case c.send <- msg:
		default:
			// slow client — drop frame
		}
	}
}

// ─── WebSocket upgrader ───────────────────────────────────────────────────────

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true }, // allow all origins
}

// ─── Redis subscriber ─────────────────────────────────────────────────────────
// Subscribes to the two channels published by telemetry-service and scoring-service.

func startRedisSubscriber(ctx context.Context, rdb *redis.Client, hub *Hub) {
	channels := []string{
		"leaderboard:updates", // published by scoring-service every 5s
		"telemetry:updates",   // published by telemetry-service every 500ms
	}

	for {
		sub := rdb.Subscribe(ctx, channels...)
		log.Printf("Redis Pub/Sub subscribed to: %v", channels)

		ch := sub.Channel()
		for msg := range ch {
			hub.Broadcast([]byte(msg.Payload))
		}

		sub.Close()
		if ctx.Err() != nil {
			return
		}
		log.Printf("Redis Pub/Sub disconnected, reconnecting in 2s...")
		time.Sleep(2 * time.Second)
	}
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

// streamHandler upgrades the connection to WebSocket and registers the client.
func streamHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("upgrade error: %v", err)
			return
		}

		client := &Client{
			id:   uuid.New().String(),
			conn: conn,
			send: make(chan []byte, 256),
		}
		hub.Register(client)

		// Write pump — sends queued messages to the browser
		go func() {
			defer func() {
				hub.Unregister(client.id)
				conn.Close()
			}()
			for msg := range client.send {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					return
				}
			}
		}()

		// Read pump — keeps connection alive, handles client pings
		defer func() {
			close(client.send)
		}()
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		// Ping ticker
		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()

		go func() {
			for range pingTicker.C {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}()

		// Block until client disconnects
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		}
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	h := map[string]interface{}{"status": "ok", "service": "ws-gateway"}
	json.NewEncoder(w).Encode(h)
}

// statsHandler returns current connection count.
func statsHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hub.mu.RLock()
		count := len(hub.clients)
		hub.mu.RUnlock()
		json.NewEncoder(w).Encode(map[string]int{"connections": count})
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	port := getenv("PORT", "8080")
	redisURL := getenv("REDIS_URL", "redis:6379")

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ping Redis
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Printf("WARNING: Redis not reachable at %s: %v — will retry", redisURL, err)
	}

	hub := NewHub()

	// Start Redis subscriber in background
	go startRedisSubscriber(ctx, rdb, hub)

	// HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/stats", statsHandler(hub))

	// WebSocket endpoints — both with and without session_id path param
	// Frontend connects to /v1/stream or /v1/stream/{session_id}
	mux.HandleFunc("/v1/stream", streamHandler(hub))
	mux.HandleFunc("/v1/stream/", streamHandler(hub)) // catches /v1/stream/{session_id}

	// CORS middleware
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})

	addr := ":" + port
	log.Printf("ws-gateway listening on %s (Redis: %s)", addr, redisURL)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
