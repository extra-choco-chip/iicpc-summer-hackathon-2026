package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type LeaderboardEntry struct {
	SessionID        string    `json:"session_id"`
	SubmissionID     string    `json:"submission_id"`
	TeamName         string    `json:"team_name"`
	Language         string    `json:"language"`
	CompositeScore   float64   `json:"composite_score"`
	ThroughputScore  float64   `json:"throughput_score"`
	LatencyScore     float64   `json:"latency_score"`
	CorrectnessScore float64   `json:"correctness_score"`
	StabilityScore   float64   `json:"stability_score"`
	P50NS            int64     `json:"p50_ns"`
	P90NS            int64     `json:"p90_ns"`
	P99NS            int64     `json:"p99_ns"`
	TPS              float64   `json:"tps"`
	FillAccPct       float64   `json:"fill_acc_pct"`
	Violations       int64     `json:"violations"`
	Status           string    `json:"status"`
	UpdatedAt        time.Time `json:"updated_at"`
}

const (
	wThroughput   = 0.30
	wLatency      = 0.30
	wCorrectness  = 0.30
	wStability    = 0.10
	targetTPS     = 1_000_000.0
	lambdaLatency = 0.15
)

func computeScore(tps float64, p99NS int64, fillAccPct float64, stable bool) (composite, tScore, lScore, cScore, sScore float64) {
	tScore = math.Min(tps/targetTPS, 1.0) * 100
	p99us := float64(p99NS) / 1000.0
	lScore = 100.0 * math.Exp(-lambdaLatency*(p99us/100.0))
	cScore = fillAccPct
	sScore = 100.0
	if !stable {
		sScore = 0.0
	}
	composite = wThroughput*tScore + wLatency*lScore + wCorrectness*cScore + wStability*sScore
	return
}

type ScoringEngine struct {
	rdb  *redis.Client
	pool *pgxpool.Pool
}

func NewScoringEngine(rdb *redis.Client, pool *pgxpool.Pool) *ScoringEngine {
	return &ScoringEngine{rdb: rdb, pool: pool}
}

func (se *ScoringEngine) RunOnce(ctx context.Context) {
	keys, err := se.rdb.Keys(ctx, "metrics:*").Result()
	if err != nil {
		log.Printf("scoring: redis error: %v", err)
		return
	}
	for _, key := range keys {
		se.scoreSession(ctx, key[len("metrics:"):])
	}
	se.publishLeaderboard(ctx)
}

func (se *ScoringEngine) scoreSession(ctx context.Context, sessionID string) {
	vals, err := se.rdb.HGetAll(ctx, "metrics:"+sessionID).Result()
	if err != nil || len(vals) == 0 {
		return
	}

	var tps, fillAcc float64
	var p50, p90, p99, violations int64
	fmt.Sscanf(vals["tps"], "%f", &tps)
	fmt.Sscanf(vals["fill_acc_pct"], "%f", &fillAcc)
	fmt.Sscanf(vals["p50_ns"], "%d", &p50)
	fmt.Sscanf(vals["p90_ns"], "%d", &p90)
	fmt.Sscanf(vals["p99_ns"], "%d", &p99)
	fmt.Sscanf(vals["violations"], "%d", &violations)

	composite, tScore, lScore, cScore, sScore := computeScore(tps, p99, fillAcc, true)

	teamName, _ := se.rdb.HGet(ctx, "session_meta:"+sessionID, "team_name").Result()
	language, _ := se.rdb.HGet(ctx, "session_meta:"+sessionID, "language").Result()
	if teamName == "" {
		teamName = "unknown"
	}

	e := &LeaderboardEntry{
		SessionID: sessionID, TeamName: teamName, Language: language,
		CompositeScore: composite, ThroughputScore: tScore,
		LatencyScore: lScore, CorrectnessScore: cScore, StabilityScore: sScore,
		P50NS: p50, P90NS: p90, P99NS: p99,
		TPS: tps, FillAccPct: fillAcc, Violations: violations,
		Status: "running", UpdatedAt: time.Now(),
	}

	data, _ := json.Marshal(e)
	se.rdb.Set(ctx, "score:"+sessionID, string(data), 2*time.Hour)
	se.rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: composite, Member: sessionID})

	if se.pool != nil {
		_, _ = se.pool.Exec(ctx,
			`INSERT INTO session_metrics
			   (time,session_id,p50_ns,p90_ns,p99_ns,tps,fill_acc_pct,violations,composite_score)
			 VALUES(NOW(),$1,$2,$3,$4,$5,$6,$7,$8)`,
			sessionID, p50, p90, p99, tps, fillAcc, violations, composite)
	}
}

func (se *ScoringEngine) publishLeaderboard(ctx context.Context) {
	results, err := se.rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, 49).Result()
	if err != nil {
		return
	}
	entries := make([]*LeaderboardEntry, 0, len(results))
	for _, r := range results {
		sid, _ := r.Member.(string)
		raw, err := se.rdb.Get(ctx, "score:"+sid).Result()
		if err != nil {
			continue
		}
		var e LeaderboardEntry
		if json.Unmarshal([]byte(raw), &e) == nil {
			entries = append(entries, &e)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CompositeScore > entries[j].CompositeScore
	})
	lb, _ := json.Marshal(map[string]interface{}{
		"entries": entries, "count": len(entries), "updated_at": time.Now(),
	})
	se.rdb.Set(ctx, "leaderboard:snapshot", string(lb), 30*time.Second)
	se.rdb.Publish(ctx, "leaderboard:updates", string(lb))
}

type Handler struct {
	engine *ScoringEngine
	rdb    *redis.Client
}

func (h *Handler) GetLeaderboard(c *gin.Context) {
	raw, err := h.rdb.Get(c.Request.Context(), "leaderboard:snapshot").Result()
	if err != nil {
		h.engine.publishLeaderboard(c.Request.Context())
		raw, err = h.rdb.Get(c.Request.Context(), "leaderboard:snapshot").Result()
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"entries": []interface{}{}, "count": 0})
			return
		}
	}
	c.Header("Content-Type", "application/json")
	c.String(http.StatusOK, raw)
}

func (h *Handler) GetScore(c *gin.Context) {
	raw, err := h.rdb.Get(c.Request.Context(), "score:"+c.Param("session_id")).Result()
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.Header("Content-Type", "application/json")
	c.String(http.StatusOK, raw)
}

func (h *Handler) GetMetrics(c *gin.Context) {
	vals, err := h.rdb.HGetAll(c.Request.Context(), "metrics:"+c.Param("session_id")).Result()
	if err != nil || len(vals) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, vals)
}

func main() {
	redisURL := getenv("REDIS_URL", "redis:6379")
	tsDSN := getenv("TIMESCALE_DSN", "postgresql://ts_user:ts_secret@timescaledb:5432/telemetry")
	port := getenv("PORT", "8080")

	var intervalSecs int
	fmt.Sscanf(getenv("SCORE_INTERVAL_SECONDS", "5"), "%d", &intervalSecs)
	if intervalSecs < 1 {
		intervalSecs = 5
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pool *pgxpool.Pool
	if p, err := pgxpool.New(ctx, tsDSN); err == nil {
		if p.Ping(ctx) == nil {
			pool = p
			log.Printf("scoring-service: TimescaleDB connected")
		}
	}
	if pool == nil {
		log.Printf("scoring-service: running without TimescaleDB")
	}

	engine := NewScoringEngine(rdb, pool)
	h := &Handler{engine: engine, rdb: rdb}

	go func() {
		t := time.NewTicker(time.Duration(intervalSecs) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				engine.RunOnce(ctx)
			}
		}
	}()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(func(c *gin.Context) { c.Header("Access-Control-Allow-Origin", "*"); c.Next() })

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "scoring-service"})
	})
	r.GET("/v1/leaderboard", h.GetLeaderboard)
	r.GET("/v1/scores/:session_id", h.GetScore)
	r.GET("/v1/metrics/:session_id", h.GetMetrics)

	log.Printf("scoring-service :%s interval=%ds", port, intervalSecs)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("scoring-service: %v", err)
	}
}
