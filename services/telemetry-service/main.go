package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
	FillPrice    float64 `json:"fill_price"`
	FillQty      float64 `json:"fill_qty"`
	SeqNo        int64   `json:"seq_no"`
	Violation    bool    `json:"violation"`
	ViolationMsg string  `json:"violation_msg"`
}

// ─── Order book mirror ────────────────────────────────────────────────────────
// Maintains a CLOB per session to validate price-time priority.

type PriceLevel struct {
	Price  float64
	Orders []*LiveOrder // FIFO per level
}

type LiveOrder struct {
	OrderID   string
	SeqNo     int64
	Qty       float64
	Timestamp int64
}

type OrderBook struct {
	mu      sync.RWMutex
	bids    []*PriceLevel // descending price
	asks    []*PriceLevel // ascending price
	orderIndex map[string]*LiveOrder
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		orderIndex: make(map[string]*LiveOrder),
	}
}

// AddOrder inserts an order into the book.
func (ob *OrderBook) AddOrder(e *TelemetryEvent) {
	if e.OrderType != "L" {
		return
	}
	ob.mu.Lock()
	defer ob.mu.Unlock()

	lo := &LiveOrder{
		OrderID:   e.OrderID,
		SeqNo:     e.SeqNo,
		Qty:       e.Qty,
		Timestamp: e.TSendNS,
	}
	ob.orderIndex[e.OrderID] = lo

	if e.Side == "B" {
		ob.insertBid(e.Price, lo)
	} else {
		ob.insertAsk(e.Price, lo)
	}
}

func (ob *OrderBook) insertBid(price float64, lo *LiveOrder) {
	for _, lvl := range ob.bids {
		if lvl.Price == price {
			lvl.Orders = append(lvl.Orders, lo)
			return
		}
	}
	ob.bids = append(ob.bids, &PriceLevel{Price: price, Orders: []*LiveOrder{lo}})
	sort.Slice(ob.bids, func(i, j int) bool { return ob.bids[i].Price > ob.bids[j].Price })
}

func (ob *OrderBook) insertAsk(price float64, lo *LiveOrder) {
	for _, lvl := range ob.asks {
		if lvl.Price == price {
			lvl.Orders = append(lvl.Orders, lo)
			return
		}
	}
	ob.asks = append(ob.asks, &PriceLevel{Price: price, Orders: []*LiveOrder{lo}})
	sort.Slice(ob.asks, func(i, j int) bool { return ob.asks[i].Price < ob.asks[j].Price })
}

func (ob *OrderBook) RemoveOrder(orderID string) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	delete(ob.orderIndex, orderID)
	// Prune from levels (lazy - we don't need perfect book state for validation)
}

// BestBid returns the best valid bid price.
func (ob *OrderBook) BestBid() float64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	for _, lvl := range ob.bids {
		for _, lo := range lvl.Orders {
			if _, exists := ob.orderIndex[lo.OrderID]; exists {
				return lvl.Price // First valid order defines the true top of book
			}
		}
	}
	return 0
}

// BestAsk returns the best valid ask price.
func (ob *OrderBook) BestAsk() float64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	for _, lvl := range ob.asks {
		for _, lo := range lvl.Orders {
			if _, exists := ob.orderIndex[lo.OrderID]; exists {
				return lvl.Price // First valid order defines the true top of book
			}
		}
	}
	return math.MaxFloat64
}

// ─── Correctness validator ────────────────────────────────────────────────────

type Validator struct {
	books map[string]*OrderBook // session_id → book
	mu    sync.Mutex
}

func NewValidator() *Validator {
	return &Validator{books: make(map[string]*OrderBook)}
}

func (v *Validator) getBook(sessionID string) *OrderBook {
	v.mu.Lock()
	defer v.mu.Unlock()
	if b, ok := v.books[sessionID]; ok {
		return b
	}
	b := NewOrderBook()
	v.books[sessionID] = b
	return b
}

// Validate checks price-time priority and fill correctness.
// Modifies e.Violation and e.ViolationMsg in place.
func (v *Validator) Validate(e *TelemetryEvent) {
	book := v.getBook(e.SessionID)

	switch e.EventType {
	case "ACK":
		// New order acknowledged — add to book mirror
		book.AddOrder(e)

	case "FILL":
		bestBid := book.BestBid()
		bestAsk := book.BestAsk()

		// Price priority: a sell fill should be at or above best bid;
		// a buy fill should be at or below best ask.
		if e.Side == "S" && bestBid > 0 && e.FillPrice < bestBid-0.001 {
			e.Violation = true
			e.ViolationMsg = fmt.Sprintf("price priority violated: sell filled at %.4f, best bid was %.4f",
				e.FillPrice, bestBid)
		}
		if e.Side == "B" && bestAsk < math.MaxFloat64 && e.FillPrice > bestAsk+0.001 {
			e.Violation = true
			e.ViolationMsg = fmt.Sprintf("price priority violated: buy filled at %.4f, best ask was %.4f",
				e.FillPrice, bestAsk)
		}

		// Fill qty sanity check
		if e.FillQty > e.Qty+0.0001 {
			e.Violation = true
			e.ViolationMsg = fmt.Sprintf("overfill: fill_qty=%.4f > order_qty=%.4f", e.FillQty, e.Qty)
		}

		book.RemoveOrder(e.OrderID)

	case "CANCEL":
		book.RemoveOrder(e.OrderID)

	case "REJECT":
		// Rejections are valid; just record
	}
}

// ─── Metrics aggregator ───────────────────────────────────────────────────────

type SessionMetrics struct {
	mu         sync.Mutex
	latencies  []int64
	totalOrders int64
	violations  int64
	lastFlush  time.Time
}

func (m *SessionMetrics) Record(latencyNS int64, violation bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if latencyNS > 0 {
		m.latencies = append(m.latencies, latencyNS)
	}
	m.totalOrders++
	if violation {
		m.violations++
	}
}

func (m *SessionMetrics) Percentile(p float64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latencies) == 0 {
		return 0
	}
	sorted := make([]int64, len(m.latencies))
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (m *SessionMetrics) TPS(windowSecs float64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return float64(m.totalOrders) / windowSecs
}

// ─── TimescaleDB writer ───────────────────────────────────────────────────────

type TSWriter struct {
	pool  *pgxpool.Pool
	batch []TelemetryEvent
	mu    sync.Mutex
}

func NewTSWriter(ctx context.Context, dsn string) (*TSWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	w := &TSWriter{pool: pool}
	go w.flushLoop()
	return w, nil
}

func (w *TSWriter) Add(e TelemetryEvent) {
	w.mu.Lock()
	w.batch = append(w.batch, e)
	shouldFlush := len(w.batch) >= 5000
	w.mu.Unlock()
	if shouldFlush {
		w.Flush()
	}
}

func (w *TSWriter) flushLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		w.Flush()
	}
}

func (w *TSWriter) Flush() {
	w.mu.Lock()
	if len(w.batch) == 0 || w.pool == nil {
		w.mu.Unlock()
		return
	}
	toFlush := w.batch
	w.batch = make([]TelemetryEvent, 0, 5000)
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows := make([][]interface{}, len(toFlush))
	for i, e := range toFlush {
		tAck := time.Now() // use current time if t_ack not set
		if e.TAckNS > 0 {
			tAck = time.Unix(0, e.TAckNS)
		}
		rows[i] = []interface{}{
			tAck,
			e.SessionID,
			e.SeqNo,
			e.OrderID,
			e.Side,
			e.OrderType,
			e.Price,
			e.Qty,
			e.TSendNS,
			e.TAckNS,
			e.LatencyNS,
			e.EventType,
			e.FillPrice,
			e.FillQty,
			e.Violation,
			e.ViolationMsg,
		}
	}

	_, err := w.pool.CopyFrom(ctx,
		pgx.Identifier{"orders"},
		[]string{"time", "session_id", "seq_no", "order_id", "side", "order_type",
			"price", "qty", "t_send_ns", "t_ack_ns", "latency_ns", "status",
			"fill_price", "fill_qty", "violation", "violation_msg"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		log.Printf("TSWriter flush error: %v", err)
	} else {
		log.Printf("TSWriter flushed %d events", len(toFlush))
	}
}

// ─── Real-time metrics publisher (→ Redis) ────────────────────────────────────

type MetricsPublisher struct {
	rdb      *redis.Client
	sessions map[string]*SessionMetrics
	mu       sync.Mutex
	start    time.Time
}

func NewMetricsPublisher(rdb *redis.Client) *MetricsPublisher {
	p := &MetricsPublisher{
		rdb:      rdb,
		sessions: make(map[string]*SessionMetrics),
		start:    time.Now(),
	}
	go p.publishLoop()
	return p
}

func (p *MetricsPublisher) Record(sessionID string, latencyNS int64, violation bool) {
	p.mu.Lock()
	m, ok := p.sessions[sessionID]
	if !ok {
		m = &SessionMetrics{lastFlush: time.Now()}
		p.sessions[sessionID] = m
	}
	p.mu.Unlock()
	m.Record(latencyNS, violation)
}

func (p *MetricsPublisher) publishLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	ctx := context.Background()

	for range ticker.C {
		elapsed := time.Since(p.start).Seconds()
		p.mu.Lock()
		for sid, m := range p.sessions {
			p50 := m.Percentile(0.50)
			p90 := m.Percentile(0.90)
			p99 := m.Percentile(0.99)
			tps := m.TPS(elapsed)

			m.mu.Lock()
			violations := m.violations
			total := m.totalOrders
			m.mu.Unlock()

			var fillAcc float64
			if total > 0 {
				fillAcc = float64(total-violations) / float64(total) * 100
			}

			// Write to Redis hash
			key := fmt.Sprintf("metrics:%s", sid)
			p.rdb.HSet(ctx, key,
				"p50_ns", p50,
				"p90_ns", p90,
				"p99_ns", p99,
				"tps", fmt.Sprintf("%.1f", tps),
				"total_orders", total,
				"violations", violations,
				"fill_acc_pct", fmt.Sprintf("%.4f", fillAcc),
				"updated_at", time.Now().Unix(),
			)
			p.rdb.Expire(ctx, key, 2*time.Hour)

			// Publish to leaderboard channel for ws-gateway
			snapshot := map[string]interface{}{
				"session_id":   sid,
				"p50_ns":       p50,
				"p90_ns":       p90,
				"p99_ns":       p99,
				"tps":          tps,
				"total_orders": total,
				"violations":   violations,
				"fill_acc_pct": fillAcc,
			}
			data, _ := json.Marshal(snapshot)
			p.rdb.Publish(ctx, "telemetry:updates", string(data))
		}
		p.mu.Unlock()
	}
}

// ─── Consumer ─────────────────────────────────────────────────────────────────

type Consumer struct {
	reader    *kafka.Reader
	validator *Validator
	tsWriter  *TSWriter
	metrics   *MetricsPublisher
}

func NewConsumer(brokers []string, topic, groupID string, tsWriter *TSWriter, metrics *MetricsPublisher) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
		ErrorLogger: kafka.LoggerFunc(func(s string, a ...interface{}) {
			log.Printf("kafka reader: "+s, a...)
		}),
	})
	return &Consumer{
		reader:    r,
		validator: NewValidator(),
		tsWriter:  tsWriter,
		metrics:   metrics,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	log.Printf("Telemetry consumer started")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("kafka fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var e TelemetryEvent
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			c.reader.CommitMessages(ctx, msg)
			continue
		}

		// Correctness validation
		c.validator.Validate(&e)

		// Write to TimescaleDB
		if c.tsWriter != nil {
			c.tsWriter.Add(e)
		}

		// Update in-memory metrics
		c.metrics.Record(e.SessionID, e.LatencyNS, e.Violation)

		c.reader.CommitMessages(ctx, msg)
	}
}

func (c *Consumer) Close() {
	c.reader.Close()
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func ensureTopic(brokers []string, topic string) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("topic setup: could not dial broker: %v", err)
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Printf("topic setup: could not get controller: %v", err)
		return
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		log.Printf("topic setup: could not dial controller: %v", err)
		return
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     4,
		ReplicationFactor: 1,
	})
	if err != nil && !isTopicExists(err) {
		log.Printf("topic setup: create topic error: %v", err)
		return
	}
	log.Printf("topic ready: %s", topic)
}

func isTopicExists(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "kafka server: Topic with this name already exists" ||
		err == kafka.TopicAlreadyExists
}

func main() {
	brokers := getenv("REDPANDA_BROKERS", "redpanda:9092")
	topic := getenv("TELEMETRY_TOPIC", "raw.telemetry")
	groupID := getenv("KAFKA_CONSUMER_GROUP", "telemetry-cg-1")
	tsDSN := getenv("TIMESCALE_DSN", "postgresql://ts_user:ts_secret@timescaledb:5432/telemetry")
	redisURL := getenv("REDIS_URL", "redis:6379")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ensure topic exists before starting consumer
	ensureTopic([]string{brokers}, topic)

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})

	var tsWriter *TSWriter
	tsWriter, err := NewTSWriter(ctx, tsDSN)
	if err != nil {
		log.Printf("WARNING: TimescaleDB not reachable: %v — running without persistence", err)
	}

	metrics := NewMetricsPublisher(rdb)
	consumer := NewConsumer([]string{brokers}, topic, groupID, tsWriter, metrics)
	defer consumer.Close()

	log.Printf("telemetry-service starting: brokers=%s topic=%s group=%s", brokers, topic, groupID)
	consumer.Run(ctx)
}
