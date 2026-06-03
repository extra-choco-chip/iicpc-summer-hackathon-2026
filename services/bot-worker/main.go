package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// ─── Config ───────────────────────────────────────────────────────────────────

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─── Order types ─────────────────────────────────────────────────────────────

type OrderSide string
type OrderType string

const (
	Buy  OrderSide = "B"
	Sell OrderSide = "S"

	Limit  OrderType = "L"
	Market OrderType = "M"
	Cancel OrderType = "C"
)

type Order struct {
	OrderID   string    `json:"order_id"`
	SessionID string    `json:"session_id"`
	Side      OrderSide `json:"side"`
	Type      OrderType `json:"type"`
	Price     float64   `json:"price,omitempty"`
	Qty       float64   `json:"qty"`
	SeqNo     int64     `json:"seq_no"`
	Timestamp int64     `json:"timestamp_ns"`
}

type OrderResponse struct {
	OrderID   string  `json:"order_id"`
	Status    string  `json:"status"`   // ACK, FILL, PARTIAL, CANCEL, REJECT
	FillPrice float64 `json:"fill_price,omitempty"`
	FillQty   float64 `json:"fill_qty,omitempty"`
	SeqNo     int64   `json:"seq_no,omitempty"`
	Timestamp int64   `json:"timestamp_ns,omitempty"`
}

// ─── Telemetry event ──────────────────────────────────────────────────────────

type TelemetryEvent struct {
	SessionID    string  `json:"session_id"`
	OrderID      string  `json:"order_id"`
	EventType    string  `json:"event_type"`
	TSendNS      int64   `json:"t_send_ns"`
	TAckNS       int64   `json:"t_ack_ns"`
	LatencyNS    int64   `json:"latency_ns"`
	Side         string  `json:"side"`
	OrderType    string  `json:"order_type"`
	Price        float64 `json:"price"`
	Qty          float64 `json:"qty"`
	FillPrice    float64 `json:"fill_price,omitempty"`
	FillQty      float64 `json:"fill_qty,omitempty"`
	SeqNo        int64   `json:"seq_no"`
	Violation    bool    `json:"violation"`
	ViolationMsg string  `json:"violation_msg,omitempty"`
}

// ─── Market simulator (synthetic price feed) ──────────────────────────────────

type MarketSim struct {
	mu        sync.RWMutex
	midPrice  float64
	spread    float64
	tickSize  float64
	seqNo     int64
}

func NewMarketSim(initialPrice float64) *MarketSim {
	return &MarketSim{
		midPrice: initialPrice,
		spread:   0.02,
		tickSize: 0.01,
	}
}

func (m *MarketSim) NextSeq() int64 {
	return atomic.AddInt64(&m.seqNo, 1)
}

// Tick simulates a random walk on the mid-price.
func (m *MarketSim) Tick() {
	m.mu.Lock()
	defer m.mu.Unlock()
	change := (rand.Float64() - 0.499) * 0.05
	m.midPrice = math.Max(1.0, m.midPrice*(1+change))
}

func (m *MarketSim) BestBid() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return math.Round((m.midPrice-m.spread/2)/m.tickSize) * m.tickSize
}

func (m *MarketSim) BestAsk() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return math.Round((m.midPrice+m.spread/2)/m.tickSize) * m.tickSize
}

func (m *MarketSim) Mid() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.midPrice
}

// ─── Participant types ────────────────────────────────────────────────────────

type ParticipantType int

const (
	MarketMaker    ParticipantType = iota // continuous 2-sided quotes
	MomentumTrader                        // burst market orders
	IcebergBot                            // hidden large limits
	SniperBot                             // aggressive at crossed quotes
	RetailFlow                            // random walk, low frequency
)

type Bot struct {
	id          string
	ptype       ParticipantType
	sessionID   string
	targetURL   string
	endpointType string
	market      *MarketSim
	producer    *TelemetryProducer
	rng         *rand.Rand

	openOrders  map[string]*Order
	ordersMu    sync.Mutex
	ordersSent  int64
}

func NewBot(id string, ptype ParticipantType, sessionID, targetURL, epType string,
	market *MarketSim, producer *TelemetryProducer) *Bot {
	return &Bot{
		id:           id,
		ptype:        ptype,
		sessionID:    sessionID,
		targetURL:    targetURL,
		endpointType: epType,
		market:       market,
		producer:     producer,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(id)))),
		openOrders:   make(map[string]*Order),
	}
}

// Run starts the bot's trading loop until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) {
	switch b.endpointType {
	case "WebSocket":
		b.runWS(ctx)
	default:
		b.runREST(ctx)
	}
}

// runWS connects via WebSocket and trades until ctx is done.
func (b *Bot) runWS(ctx context.Context) {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, b.targetURL, nil)
	if err != nil {
		log.Printf("Bot %s: WS dial failed: %v", b.id[:8], err)
		// Fall back to REST
		b.runREST(ctx)
		return
	}
	defer conn.Close()

	// Read goroutine
	acks := make(chan OrderResponse, 512)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var resp OrderResponse
			if json.Unmarshal(msg, &resp) == nil {
				acks <- resp
			}
		}
	}()

	// Pending orders waiting for ack: orderID → tSendNS
	pending := make(map[string]struct {
		order   *Order
		tSendNS int64
	})

	ticker := time.NewTicker(b.thinkTime())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-acks:
			if p, ok := pending[ack.OrderID]; ok {
				tAck := time.Now().UnixNano()
				latency := tAck - p.tSendNS
				b.producer.Emit(TelemetryEvent{
					SessionID: b.sessionID,
					OrderID:   ack.OrderID,
					EventType: ack.Status,
					TSendNS:   p.tSendNS,
					TAckNS:    tAck,
					LatencyNS: latency,
					Side:      string(p.order.Side),
					OrderType: string(p.order.Type),
					Price:     p.order.Price,
					Qty:       p.order.Qty,
					FillPrice: ack.FillPrice,
					FillQty:   ack.FillQty,
					SeqNo:     p.order.SeqNo,
				})
				delete(pending, ack.OrderID)
			}
		case <-ticker.C:
			order := b.generateOrder()
			data, err := json.Marshal(order)
			if err != nil {
				continue
			}
			tSend := time.Now().UnixNano()
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
			pending[order.OrderID] = struct {
				order   *Order
				tSendNS int64
			}{order, tSend}
			atomic.AddInt64(&b.ordersSent, 1)
			ticker.Reset(b.thinkTime())
		}
	}
}

// runREST sends orders over HTTP.
func (b *Bot) runREST(ctx context.Context) {
	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := b.targetURL

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		order := b.generateOrder()
		data, _ := json.Marshal(order)
		tSend := time.Now().UnixNano()

		req, err := http.NewRequestWithContext(ctx, "POST",
			baseURL+"/orders", bytes.NewReader(data))
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		tAck := time.Now().UnixNano()
		latency := tAck - tSend

		eventType := "ERROR"
		var fillPrice, fillQty float64
		if err == nil {
			var ack OrderResponse
			json.NewDecoder(resp.Body).Decode(&ack)
			resp.Body.Close()
			eventType = ack.Status
			if eventType == "" {
				eventType = "ACK"
			}
			fillPrice = ack.FillPrice
			fillQty = ack.FillQty
		}

		b.producer.Emit(TelemetryEvent{
			SessionID: b.sessionID,
			OrderID:   order.OrderID,
			EventType: eventType,
			TSendNS:   tSend,
			TAckNS:    tAck,
			LatencyNS: latency,
			Side:      string(order.Side),
			OrderType: string(order.Type),
			Price:     order.Price,
			Qty:       order.Qty,
			FillPrice: fillPrice,
			FillQty:   fillQty,
			SeqNo:     order.SeqNo,
		})
		atomic.AddInt64(&b.ordersSent, 1)

		time.Sleep(b.thinkTime())
	}
}

// generateOrder creates an order based on participant type.
func (b *Bot) generateOrder() *Order {
	order := &Order{
		OrderID:   uuid.New().String(),
		SessionID: b.sessionID,
		SeqNo:     b.market.NextSeq(),
		Timestamp: time.Now().UnixNano(),
	}

	switch b.ptype {
	case MarketMaker:
		// Continuous two-sided limit orders
		if b.rng.Float64() < 0.5 {
			order.Side = Buy
			order.Type = Limit
			order.Price = b.market.BestBid() - float64(b.rng.Intn(3))*0.01
		} else {
			order.Side = Sell
			order.Type = Limit
			order.Price = b.market.BestAsk() + float64(b.rng.Intn(3))*0.01
		}
		order.Qty = float64(b.rng.Intn(100)+1) * 10

	case MomentumTrader:
		// Aggressive market orders on momentum
		order.Type = Market
		if b.rng.Float64() < 0.6 {
			order.Side = Buy
		} else {
			order.Side = Sell
		}
		order.Qty = float64(b.rng.Intn(500)+100) * 10

	case IcebergBot:
		// Large hidden limit orders
		if b.rng.Float64() < 0.5 {
			order.Side = Buy
			order.Price = b.market.BestBid() * (1 - b.rng.Float64()*0.001)
		} else {
			order.Side = Sell
			order.Price = b.market.BestAsk() * (1 + b.rng.Float64()*0.001)
		}
		order.Type = Limit
		order.Qty = float64(b.rng.Intn(5000)+1000) * 10

	case SniperBot:
		// Exploit crossed quotes with market orders
		order.Type = Market
		if b.rng.Float64() < 0.5 {
			order.Side = Buy
		} else {
			order.Side = Sell
		}
		order.Qty = float64(b.rng.Intn(200)+50) * 10

	case RetailFlow:
		// Slow random limit orders
		mid := b.market.Mid()
		offset := (b.rng.Float64() - 0.5) * mid * 0.01
		order.Side = Buy
		order.Type = Limit
		if b.rng.Float64() < 0.5 {
			order.Side = Sell
		}
		order.Price = math.Max(0.01, mid+offset)
		order.Qty = float64(b.rng.Intn(50)+1) * 10
	}

	return order
}

// thinkTime returns inter-order delay based on participant type.
func (b *Bot) thinkTime() time.Duration {
	switch b.ptype {
	case MarketMaker:
		return time.Duration(500+b.rng.Intn(500)) * time.Microsecond
	case MomentumTrader:
		return time.Duration(200+b.rng.Intn(300)) * time.Microsecond
	case IcebergBot:
		return time.Duration(1+b.rng.Intn(5)) * time.Millisecond
	case SniperBot:
		return time.Duration(100+b.rng.Intn(200)) * time.Microsecond
	default: // RetailFlow
		return time.Duration(10+b.rng.Intn(90)) * time.Millisecond
	}
}

// ─── Telemetry producer ───────────────────────────────────────────────────────

type TelemetryProducer struct {
	writer  *kafka.Writer
}

func NewTelemetryProducer(brokers []string, topic string) *TelemetryProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    500,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		ErrorLogger:  kafka.LoggerFunc(func(s string, a ...interface{}) {
			log.Printf("kafka error: "+s, a...)
		}),
	}
	return &TelemetryProducer{
		writer: w,
	}
}

func (p *TelemetryProducer) Emit(e TelemetryEvent) {
	data, _ := json.Marshal(e)
	// Because Async is true, this returns immediately and does not block.
	_ = p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(e.SessionID),
		Value: data,
	})
}

func (p *TelemetryProducer) Close() error {
	return p.writer.Close()
}

// ─── Worker (manages a pool of bots for one session) ─────────────────────────

type Worker struct {
	id       string
	orchURL  string
	producer *TelemetryProducer

	mu          sync.Mutex
	activeBots  []*Bot
	ordersSent  int64
	cancelFurcs map[string]context.CancelFunc
}

func NewWorker(id, orchURL string, producer *TelemetryProducer) *Worker {
	return &Worker{
		id:          id,
		orchURL:     orchURL,
		producer:    producer,
		cancelFurcs: make(map[string]context.CancelFunc),
	}
}

// SpawnBots starts N bots targeting the given session.
func (w *Worker) SpawnBots(ctx context.Context, sessionID, targetURL, endpointType string, count int) {
	market := NewMarketSim(100.0 + rand.Float64()*900.0)
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				market.Tick()
			}
		}
	}()

	ptypes := []ParticipantType{
		MarketMaker, MarketMaker,           // 20%
		MomentumTrader, MomentumTrader, MomentumTrader, // 30% (approx)
		IcebergBot,                          // 15%
		SniperBot,                           // 10%
		RetailFlow, RetailFlow, RetailFlow,  // 25%
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 0; i < count; i++ {
		botID := fmt.Sprintf("%s-bot-%d", w.id[:8], i)
		ptype := ptypes[i%len(ptypes)]
		bot := NewBot(botID, ptype, sessionID, targetURL, endpointType, market, w.producer)
		w.activeBots = append(w.activeBots, bot)

		botCtx, cancel := context.WithCancel(ctx)
		w.cancelFurcs[botID] = cancel

		go func(b *Bot) {
			b.Run(botCtx)
		}(bot)
	}

	log.Printf("Worker %s: spawned %d bots for session %s → %s", w.id[:8], count, sessionID[:8], targetURL)
}

func (w *Worker) StopBots() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cancel := range w.cancelFurcs {
		cancel()
	}
	w.activeBots = nil
	w.cancelFurcs = make(map[string]context.CancelFunc)
}

func (w *Worker) TotalOrdersSent() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total int64
	for _, b := range w.activeBots {
		total += atomic.LoadInt64(&b.ordersSent)
	}
	return total
}

func (w *Worker) ActiveBotCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.activeBots)
}

// ─── Orchestrator poller ──────────────────────────────────────────────────────

type OrchestratorPoller struct {
	workerID string
	orchURL  string
	worker   *Worker
	client   *http.Client
}

func NewOrchestratorPoller(workerID, orchURL string, worker *Worker) *OrchestratorPoller {
	return &OrchestratorPoller{
		workerID: workerID,
		orchURL:  orchURL,
		worker:   worker,
		client:   &http.Client{Timeout: 35 * time.Second},
	}
}

type WorkerCmd struct {
	Command      string `json:"command"`
	SessionID    string `json:"session_id"`
	TargetURL    string `json:"target_url"`
	BotCount     int    `json:"bot_count"`
	EndpointType string `json:"endpoint_type"`
}

func (p *OrchestratorPoller) Poll(ctx context.Context) {
	log.Printf("Worker %s: polling orchestrator at %s", p.workerID[:8], p.orchURL)
	var currentSessionID string
	var sessionMu sync.Mutex

	// Heartbeat runs independently — never blocked by the long-poll HTTP call
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sessionMu.Lock()
				sid := currentSessionID
				sessionMu.Unlock()
				p.sendHeartbeat(ctx, sid)
			}
		}
	}()

	var currentSessionCancel context.CancelFunc
	var currentSessionCtx context.Context

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Long-poll for commands
		req, err := http.NewRequestWithContext(ctx, "GET",
			fmt.Sprintf("%s/api/workers/%s/poll", p.orchURL, p.workerID), nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		resp, err := p.client.Do(req)
		if err != nil {
			log.Printf("Worker %s: poll error: %v, retrying in 3s", p.workerID[:8], err)
			time.Sleep(3 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			continue
		}

		var cmd WorkerCmd
		if err := json.NewDecoder(resp.Body).Decode(&cmd); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		switch cmd.Command {
		case "START":
			if currentSessionCancel != nil {
				currentSessionCancel()
			}
			currentSessionCtx, currentSessionCancel = context.WithCancel(ctx)
			sessionMu.Lock()
			currentSessionID = cmd.SessionID
			sessionMu.Unlock()
			botCount := cmd.BotCount
			if botCount == 0 {
				botCount = 8
			}
			go p.worker.SpawnBots(currentSessionCtx, cmd.SessionID, cmd.TargetURL, cmd.EndpointType, botCount)

		case "STOP":
			if currentSessionCancel != nil {
				currentSessionCancel()
			}
			p.worker.StopBots()
			sessionMu.Lock()
			currentSessionID = ""
			sessionMu.Unlock()
			currentSessionCtx = nil
			currentSessionCancel = nil
		}
	}
}

func (p *OrchestratorPoller) sendHeartbeat(ctx context.Context, sessionID string) {
	hb := map[string]interface{}{
		"worker_id":   p.workerID,
		"session_id":  sessionID,
		"active_bots": p.worker.ActiveBotCount(),
		"orders_sent": p.worker.TotalOrdersSent(),
	}
	data, _ := json.Marshal(hb)
	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/api/workers/%s/heartbeat", p.orchURL, p.workerID),
		bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

// ─── Topic setup ─────────────────────────────────────────────────────────────

func ensureTopic(broker, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("topic setup: dial error: %v (will retry via producer)", err)
		return
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return
	}
	cc, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return
	}
	defer cc.Close()
	cc.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     4,
		ReplicationFactor: 1,
	})
	log.Printf("topic ready: %s", topic)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	orchURL := getenv("ORCHESTRATOR_URL", "http://bot-orchestrator:9090")
	brokers := getenv("REDPANDA_BROKERS", "redpanda:9092")
	topic := getenv("TELEMETRY_TOPIC", "raw.telemetry")
	workerID := getenv("WORKER_ID", uuid.New().String())

	ensureTopic(brokers, topic)

	producer := NewTelemetryProducer([]string{brokers}, topic)
	defer producer.Close()

	worker := NewWorker(workerID, orchURL, producer)
	poller := NewOrchestratorPoller(workerID, orchURL, worker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("bot-worker %s starting, orchestrator=%s, brokers=%s", workerID[:8], orchURL, brokers)
	poller.Poll(ctx)
}
