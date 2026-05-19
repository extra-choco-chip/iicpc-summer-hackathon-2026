package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ─── Config ──────────────────────────────────────────────────────────────────

type Config struct {
	Port           string
	JWTSecret      string
	SubmissionSvc  string
	WsGateway      string
	ScoringURL     string
	RedisURL       string
}

func configFromEnv() Config {
	return Config{
		Port:          getenv("PORT", "8080"),
		JWTSecret:     getenv("JWT_SECRET", "dev-secret-change-in-prod"),
		SubmissionSvc: getenv("SUBMISSION_SVC", "http://submission-service:8080"),
		WsGateway:     getenv("WS_GATEWAY", "http://ws-gateway:8080"),
		ScoringURL:    getenv("SCORING_URL", "http://scoring-service:8080"),
		RedisURL:      getenv("REDIS_URL", "redis:6379"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Rate limiter (token bucket via Redis) ────────────────────────────────────

type RateLimiter struct {
	rdb *redis.Client
}

func NewRateLimiter(addr string) *RateLimiter {
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 1})
	return &RateLimiter{rdb: rdb}
}

// Allow returns true if the key is within rate limit (requests/window).
func (r *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) bool {
	pipe := r.rdb.Pipeline()
	now := time.Now().UnixMilli()
	windowMs := window.Milliseconds()

	k := fmt.Sprintf("rl:%s", key)
	pipe.ZRemRangeByScore(ctx, k, "0", fmt.Sprintf("%d", now-windowMs))
	pipe.ZCard(ctx, k)
	pipe.ZAdd(ctx, k, redis.Z{Score: float64(now), Member: uuid.New().String()})
	pipe.Expire(ctx, k, window)

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return true // fail open on Redis error
	}
	count := cmds[1].(*redis.IntCmd).Val()
	return count < int64(limit)
}

// ─── JWT middleware ───────────────────────────────────────────────────────────

func JWTMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing Authorization header"})
			return
		}
		tokenStr := strings.TrimPrefix(header, "Bearer ")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(secret), nil
		})
		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		claims := token.Claims.(jwt.MapClaims)
		c.Set("team_id", claims["sub"])
		c.Set("team_name", claims["team_name"])
		c.Next()
	}
}

// ─── Rate limit middleware ────────────────────────────────────────────────────

func RateLimitMiddleware(rl *RateLimiter, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.ClientIP()
		if tid, ok := c.Get("team_id"); ok {
			key = fmt.Sprintf("%v", tid)
		}
		if !rl.Allow(c.Request.Context(), key, limit, window) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded",
				"retry_after": window.Seconds(),
			})
			return
		}
		c.Next()
	}
}

// ─── Reverse proxy helpers ────────────────────────────────────────────────────

func proxyTo(targetBase string) gin.HandlerFunc {
	client := &http.Client{Timeout: 30 * time.Second}
	return func(c *gin.Context) {
		url := targetBase + c.Request.URL.RequestURI()
		req, err := http.NewRequestWithContext(c.Request.Context(),
			c.Request.Method, url, c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		// Forward headers
		for k, vv := range c.Request.Header {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
		// Inject identity headers for downstream services
		if tid, ok := c.Get("team_id"); ok {
			req.Header.Set("X-Team-ID", fmt.Sprintf("%v", tid))
		}
		if tn, ok := c.Get("team_name"); ok {
			req.Header.Set("X-Team-Name", fmt.Sprintf("%v", tn))
		}

		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()

		for k, vv := range resp.Header {
			for _, v := range vv {
				c.Header(k, v)
			}
		}
		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	}
}

// ─── Auth handlers ────────────────────────────────────────────────────────────

type AuthHandler struct {
	secret string
	rdb    *redis.Client
}

func (h *AuthHandler) Register(c *gin.Context) {
	var body struct {
		TeamName string `json:"team_name" binding:"required"`
		Password string `json:"password"  binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	teamID := uuid.New().String()

	// Store team in Redis (in production: use PostgreSQL)
	ctx := c.Request.Context()
	key := fmt.Sprintf("team:%s", body.TeamName)
	exists, _ := h.rdb.Exists(ctx, key).Result()
	if exists > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "team name already taken"})
		return
	}
	h.rdb.HSet(ctx, key, "team_id", teamID, "password", body.Password)
	h.rdb.Set(ctx, fmt.Sprintf("team_id:%s", teamID), body.TeamName, 0)

	token, err := h.issueToken(teamID, body.TeamName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"token":   token,
		"team_id": teamID,
		"expires": time.Now().Add(24 * time.Hour).Unix(),
	})
}

func (h *AuthHandler) Login(c *gin.Context) {
	var body struct {
		TeamName string `json:"team_name" binding:"required"`
		Password string `json:"password"  binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()
	key := fmt.Sprintf("team:%s", body.TeamName)
	stored, err := h.rdb.HGetAll(ctx, key).Result()
	if err != nil || stored["password"] != body.Password {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	token, err := h.issueToken(stored["team_id"], body.TeamName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"token":   token,
		"team_id": stored["team_id"],
		"expires": time.Now().Add(24 * time.Hour).Unix(),
	})
}

func (h *AuthHandler) issueToken(teamID, teamName string) (string, error) {
	claims := jwt.MapClaims{
		"sub":       teamID,
		"team_name": teamName,
		"iat":       time.Now().Unix(),
		"exp":       time.Now().Add(24 * time.Hour).Unix(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(h.secret))
}

// ─── Health check ─────────────────────────────────────────────────────────────

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "api-gateway",
		"time":    time.Now().UTC(),
	})
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := configFromEnv()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis not reachable at %s: %v", cfg.RedisURL, err)
	}

	rl := NewRateLimiter(cfg.RedisURL)
	auth := &AuthHandler{secret: cfg.JWTSecret, rdb: rdb}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// CORS
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Team-ID")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Public
	r.GET("/health", healthCheck)
	r.POST("/v1/auth/register", auth.Register)
	r.POST("/v1/auth/login", auth.Login)

	// Protected routes
	api := r.Group("/v1", JWTMiddleware(cfg.JWTSecret))

	// Submissions (rate-limited: 10 uploads per minute)
	submitLimit := RateLimitMiddleware(rl, 10, time.Minute)
	api.POST("/submissions", submitLimit, proxyTo(cfg.SubmissionSvc))
	api.GET("/submissions", proxyTo(cfg.SubmissionSvc))
	api.GET("/submissions/:id", proxyTo(cfg.SubmissionSvc))

	// Benchmark sessions
	api.POST("/sessions/:id/start", proxyTo(cfg.SubmissionSvc))
	api.POST("/sessions/:id/stop", proxyTo(cfg.SubmissionSvc))
	api.GET("/sessions/:id", proxyTo(cfg.SubmissionSvc))

	// Leaderboard + scores (public read)
	r.GET("/v1/leaderboard", proxyTo(cfg.ScoringURL))
	r.GET("/v1/scores/:session_id", proxyTo(cfg.ScoringURL))

	// WebSocket passthrough (ws-gateway handles upgrade)
	r.GET("/v1/stream/:session_id", proxyTo(cfg.WsGateway))
	r.GET("/v1/stream", proxyTo(cfg.WsGateway))

	addr := ":" + cfg.Port
	log.Printf("api-gateway listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
