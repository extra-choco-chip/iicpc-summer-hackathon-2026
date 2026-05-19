package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ─── Config ──────────────────────────────────────────────────────────────────

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─── Session model ────────────────────────────────────────────────────────────

type SessionStatus string

const (
	StatusPending  SessionStatus = "pending"
	StatusRunning  SessionStatus = "running"
	StatusStopping SessionStatus = "stopping"
	StatusDone     SessionStatus = "done"
	StatusError    SessionStatus = "error"
)

type BenchmarkSession struct {
	SessionID     string        `json:"session_id"`
	SubmissionID  string        `json:"submission_id"`
	Status        SessionStatus `json:"status"`
	TargetURL     string        `json:"target_url"`
	EndpointType  string        `json:"endpoint_type"`
	BotCount      int           `json:"bot_count"`
	DurationSecs  int           `json:"duration_secs"`
	ActiveBots    int           `json:"active_bots"`
	OrdersSent    int64         `json:"orders_sent"`
	OrdersAcked   int64         `json:"orders_acked"`
	TPS           float64       `json:"tps"`
	P50NS         int64         `json:"p50_ns"`
	P99NS         int64         `json:"p99_ns"`
	CreatedAt     time.Time     `json:"created_at"`
	StartedAt     *time.Time    `json:"started_at,omitempty"`
	EndedAt       *time.Time    `json:"ended_at,omitempty"`
}

// ─── Worker model ─────────────────────────────────────────────────────────────

type WorkerInfo struct {
	WorkerID   string
	SessionID  string
	ActiveBots int
	OrdersSent int64
	LastSeen   time.Time
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

type Orchestrator struct {
	mu       sync.RWMutex
	sessions map[string]*BenchmarkSession
	workers  map[string]*WorkerInfo
	rdb      *redis.Client

	// Channels: workerID → command channel
	cmdChans map[string]chan WorkerCmd
}

type WorkerCmd struct {
	Command      string `json:"command"`
	SessionID    string `json:"session_id"`
	TargetURL    string `json:"target_url"`
	BotCount     int    `json:"bot_count"`
	EndpointType string `json:"endpoint_type"`
}

func NewOrchestrator(rdb *redis.Client) *Orchestrator {
	o := &Orchestrator{
		sessions: make(map[string]*BenchmarkSession),
		workers:  make(map[string]*WorkerInfo),
		cmdChans: make(map[string]chan WorkerCmd),
		rdb:      rdb,
	}
	go o.reconcileLoop()
	go o.metricsAggLoop()
	return o
}

// StartSession creates a new benchmark session and dispatches to workers.
func (o *Orchestrator) StartSession(ctx context.Context, submissionID, sessionID, targetURL, endpointType string, botCount, durationSecs int) (*BenchmarkSession, error) {
	if sessionID == "" {
		sessionID = uuid.New().String()
	}

	now := time.Now()
	sess := &BenchmarkSession{
		SessionID:    sessionID,
		SubmissionID: submissionID,
		Status:       StatusPending,
		TargetURL:    targetURL,
		EndpointType: endpointType,
		BotCount:     botCount,
		DurationSecs: durationSecs,
		CreatedAt:    now,
	}

	o.mu.Lock()
	o.sessions[sessionID] = sess
	o.mu.Unlock()

	// Persist session to Redis
	data, _ := json.Marshal(sess)
	o.rdb.Set(ctx, fmt.Sprintf("session:%s", sessionID), data, time.Duration(durationSecs+300)*time.Second)

	// Dispatch to all connected workers
	go o.dispatchToWorkers(sessionID, targetURL, endpointType, botCount, durationSecs)

	return sess, nil
}

func (o *Orchestrator) dispatchToWorkers(sessionID, targetURL, endpointType string, botCount, durationSecs int) {
	// Wait briefly for workers to connect if none yet
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		o.mu.RLock()
		wCount := len(o.workers)
		o.mu.RUnlock()
		if wCount > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	workers := make([]string, 0, len(o.workers))
	for id := range o.workers {
		workers = append(workers, id)
	}

	if len(workers) == 0 {
		log.Printf("No workers connected; publishing session to Redis for pickup")
		// Workers poll Redis on startup
		cmd := WorkerCmd{
			Command:      "START",
			SessionID:    sessionID,
			TargetURL:    targetURL,
			BotCount:     botCount,
			EndpointType: endpointType,
		}
		data, _ := json.Marshal(cmd)
		o.rdb.Publish(context.Background(), "worker:broadcast", string(data))
		return
	}

	// Distribute bots evenly across workers
	botsPerWorker := botCount / len(workers)
	remainder := botCount % len(workers)

	for i, wid := range workers {
		bots := botsPerWorker
		if i < remainder {
			bots++
		}
		cmd := WorkerCmd{
			Command:      "START",
			SessionID:    sessionID,
			TargetURL:    targetURL,
			BotCount:     bots,
			EndpointType: endpointType,
		}
		if ch, ok := o.cmdChans[wid]; ok {
			select {
			case ch <- cmd:
			default:
				log.Printf("Worker %s channel full, publishing to Redis", wid)
				data, _ := json.Marshal(cmd)
				o.rdb.Publish(context.Background(), fmt.Sprintf("worker:%s", wid), string(data))
			}
		}
	}

	sess := o.sessions[sessionID]
	if sess != nil {
		sess.Status = StatusRunning
		t := time.Now()
		sess.StartedAt = &t
		sess.ActiveBots = botCount
	}

	// Auto-stop after duration
	go func() {
		time.Sleep(time.Duration(durationSecs) * time.Second)
		o.StopSession(context.Background(), sessionID)
	}()
}

func (o *Orchestrator) StopSession(ctx context.Context, sessionID string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sess, ok := o.sessions[sessionID]
	if !ok {
		return
	}
	sess.Status = StatusDone
	t := time.Now()
	sess.EndedAt = &t

	// Broadcast STOP to all workers
	cmd := WorkerCmd{Command: "STOP", SessionID: sessionID}
	data, _ := json.Marshal(cmd)
	for _, ch := range o.cmdChans {
		select {
		case ch <- cmd:
		default:
		}
	}
	o.rdb.Publish(ctx, "worker:broadcast", string(data))
	log.Printf("Session %s stopped", sessionID)
}

// RegisterWorker is called when a new worker connects.
func (o *Orchestrator) RegisterWorker(workerID string) chan WorkerCmd {
	ch := make(chan WorkerCmd, 64)
	o.mu.Lock()
	o.workers[workerID] = &WorkerInfo{WorkerID: workerID, LastSeen: time.Now()}
	o.cmdChans[workerID] = ch
	o.mu.Unlock()
	log.Printf("Worker registered: %s (total: %d)", workerID, len(o.workers))
	return ch
}

// UpdateWorkerHeartbeat processes a heartbeat from a worker.
func (o *Orchestrator) UpdateWorkerHeartbeat(hb WorkerHeartbeat) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if w, ok := o.workers[hb.WorkerID]; ok {
		w.ActiveBots = hb.ActiveBots
		w.OrdersSent = hb.OrdersSent
		w.SessionID = hb.SessionID
		w.LastSeen = time.Now()
	}

	// Aggregate into session
	if hb.SessionID != "" {
		if sess, ok := o.sessions[hb.SessionID]; ok {
			sess.OrdersSent += hb.OrdersSent
		}
	}
}

// reconcileLoop detects dead workers and reassigns their shards.
func (o *Orchestrator) reconcileLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		o.mu.Lock()
		for id, w := range o.workers {
			if time.Since(w.LastSeen) > 60*time.Second {
				log.Printf("Worker %s timed out, removing", id)
				delete(o.workers, id)
				delete(o.cmdChans, id)
				// TODO: reassign shards to remaining workers
			}
		}
		o.mu.Unlock()
	}
}

// metricsAggLoop pulls aggregated metrics from Redis and updates sessions.
func (o *Orchestrator) metricsAggLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	ctx := context.Background()
	for range ticker.C {
		o.mu.RLock()
		sessions := make([]string, 0)
		for id, s := range o.sessions {
			if s.Status == StatusRunning {
				sessions = append(sessions, id)
			}
		}
		o.mu.RUnlock()

		for _, sid := range sessions {
			key := fmt.Sprintf("metrics:%s", sid)
			vals, err := o.rdb.HGetAll(ctx, key).Result()
			if err != nil || len(vals) == 0 {
				continue
			}
			o.mu.Lock()
			if sess, ok := o.sessions[sid]; ok {
				if v, ok := vals["tps"]; ok {
					fmt.Sscanf(v, "%f", &sess.TPS)
				}
				if v, ok := vals["p50_ns"]; ok {
					fmt.Sscanf(v, "%d", &sess.P50NS)
				}
				if v, ok := vals["p99_ns"]; ok {
					fmt.Sscanf(v, "%d", &sess.P99NS)
				}
				// Persist updated session
				data, _ := json.Marshal(sess)
				o.rdb.Set(ctx, fmt.Sprintf("session:%s", sid), data, 1*time.Hour)
			}
			o.mu.Unlock()
		}
	}
}

// ─── Worker Heartbeat model ───────────────────────────────────────────────────

type WorkerHeartbeat struct {
	WorkerID   string `json:"worker_id"`
	SessionID  string `json:"session_id"`
	ActiveBots int    `json:"active_bots"`
	OrdersSent int64  `json:"orders_sent"`
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

type APIHandler struct {
	orch *Orchestrator
	rdb  *redis.Client
}

func (h *APIHandler) StartSession(c *gin.Context) {
	var req struct {
		SubmissionID string `json:"submission_id"`
		SessionID    string `json:"session_id"`
		TargetURL    string `json:"target_url"`
		EndpointType string `json:"endpoint_type"`
		BotCount     int    `json:"bot_count"`
		DurationSecs int    `json:"duration_secs"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.BotCount == 0 {
		req.BotCount = 2048
	}
	if req.DurationSecs == 0 {
		req.DurationSecs = 300
	}
	if req.EndpointType == "" {
		req.EndpointType = "WebSocket"
	}

	sess, err := h.orch.StartSession(c.Request.Context(),
		req.SubmissionID, req.SessionID, req.TargetURL,
		req.EndpointType, req.BotCount, req.DurationSecs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, sess)
}

func (h *APIHandler) StopSession(c *gin.Context) {
	sessionID := c.Param("session_id")
	h.orch.StopSession(c.Request.Context(), sessionID)
	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

func (h *APIHandler) GetSession(c *gin.Context) {
	sessionID := c.Param("session_id")
	h.orch.mu.RLock()
	sess, ok := h.orch.sessions[sessionID]
	h.orch.mu.RUnlock()
	if !ok {
		// Try Redis
		data, err := h.rdb.Get(c.Request.Context(), fmt.Sprintf("session:%s", sessionID)).Result()
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "session not found"})
			return
		}
		var s BenchmarkSession
		json.Unmarshal([]byte(data), &s)
		c.JSON(http.StatusOK, &s)
		return
	}
	c.JSON(http.StatusOK, sess)
}

func (h *APIHandler) ListSessions(c *gin.Context) {
	h.orch.mu.RLock()
	sessions := make([]*BenchmarkSession, 0, len(h.orch.sessions))
	for _, s := range h.orch.sessions {
		sessions = append(sessions, s)
	}
	h.orch.mu.RUnlock()
	c.JSON(http.StatusOK, gin.H{"sessions": sessions})
}

// WorkerHeartbeatHandler accepts POST heartbeats from bot-worker pods.
func (h *APIHandler) WorkerHeartbeat(c *gin.Context) {
	var hb WorkerHeartbeat
	if err := c.ShouldBindJSON(&hb); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	h.orch.UpdateWorkerHeartbeat(hb)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// WorkerPoll is a long-poll endpoint for workers to pick up commands.
func (h *APIHandler) WorkerPoll(c *gin.Context) {
	workerID := c.Param("worker_id")
	if workerID == "" {
		workerID = uuid.New().String()
	}

	ch := h.orch.RegisterWorker(workerID)
	// DO NOT delete the worker when the poll returns —
	// the worker immediately re-polls. Only the reconcile loop
	// evicts workers that stop polling entirely (60s timeout).

	// Refresh LastSeen so the reconcile loop knows this worker is alive
	h.orch.mu.Lock()
	if w, ok := h.orch.workers[workerID]; ok {
		w.LastSeen = time.Now()
	}
	h.orch.mu.Unlock()

	// Subscribe to Redis broadcast channel
	pubsub := h.rdb.Subscribe(c.Request.Context(), "worker:broadcast", fmt.Sprintf("worker:%s", workerID))
	defer pubsub.Close()

	// Wait up to 30s for a command
	select {
	case cmd := <-ch:
		c.JSON(http.StatusOK, cmd)
	case msg := <-pubsub.Channel():
		var cmd WorkerCmd
		json.Unmarshal([]byte(msg.Payload), &cmd)
		c.JSON(http.StatusOK, cmd)
	case <-time.After(30 * time.Second):
		c.JSON(http.StatusNoContent, nil)
	case <-c.Request.Context().Done():
		c.JSON(http.StatusNoContent, nil)
	}
}

func (h *APIHandler) GetWorkers(c *gin.Context) {
	h.orch.mu.RLock()
	workers := make([]*WorkerInfo, 0, len(h.orch.workers))
	for _, w := range h.orch.workers {
		workers = append(workers, w)
	}
	h.orch.mu.RUnlock()
	c.JSON(http.StatusOK, gin.H{"workers": workers, "count": len(workers)})
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	redisURL := getenv("REDIS_URL", "redis:6379")
	port := getenv("PORT", "9090")

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis not reachable: %v", err)
	}

	orch := NewOrchestrator(rdb)
	h := &APIHandler{orch: orch, rdb: rdb}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "bot-orchestrator",
			"workers": len(orch.workers), "sessions": len(orch.sessions)})
	})

	api := r.Group("/api")
	api.POST("/sessions/start", h.StartSession)
	api.POST("/sessions/:session_id/stop", h.StopSession)
	api.GET("/sessions/:session_id", h.GetSession)
	api.GET("/sessions", h.ListSessions)
	api.GET("/workers", h.GetWorkers)

	// Worker endpoints
	api.POST("/workers/:worker_id/heartbeat", h.WorkerHeartbeat)
	api.GET("/workers/:worker_id/poll", h.WorkerPoll)

	log.Printf("bot-orchestrator listening on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
