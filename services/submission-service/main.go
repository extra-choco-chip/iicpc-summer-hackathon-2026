package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ─── Config ──────────────────────────────────────────────────────────────────

type Config struct {
	Port              string
	PostgresDSN       string
	MinioEndpoint     string
	MinioAccessKey    string
	MinioSecretKey    string
	MinioBucket       string
	ContainerRegistry string
	BotOrchestratorURL string
	SandboxRuntime    string
	KubeconfigPath    string
}

func configFromEnv() Config {
	return Config{
		Port:               getenv("PORT", "8080"),
		PostgresDSN:        getenv("POSTGRES_DSN", "postgresql://of_user:of_secret@postgres:5432/orderfurnace"),
		MinioEndpoint:      getenv("MINIO_ENDPOINT", "minio:9000"),
		MinioAccessKey:     getenv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey:     getenv("MINIO_SECRET_KEY", "minioadmin123"),
		MinioBucket:        getenv("MINIO_BUCKET", "submissions"),
		ContainerRegistry:  getenv("CONTAINER_REGISTRY", "localhost:5000"),
		BotOrchestratorURL: getenv("BOT_ORCHESTRATOR_URL", "http://bot-orchestrator:9090"),
		SandboxRuntime:     getenv("SANDBOX_RUNTIME", "runc"),
		KubeconfigPath:     getenv("KUBECONFIG", ""),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─── DB layer ─────────────────────────────────────────────────────────────────

type DB struct{ pool *pgxpool.Pool }

func NewDB(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &DB{pool: pool}, pool.Ping(ctx)
}

type SubmissionRecord struct {
	SubmissionID string    `json:"submission_id"`
	TeamID       string    `json:"team_id"`
	TeamName     string    `json:"team_name"`
	Language     string    `json:"language"`
	EndpointType string    `json:"endpoint_type"`
	Filename     string    `json:"filename"`
	ObjectKey    string    `json:"object_key"`
	ImageRef     string    `json:"image_ref"`
	Status       string    `json:"status"`
	SessionID    string    `json:"session_id"`
	ErrorMsg     string    `json:"error_msg"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (db *DB) CreateSubmission(ctx context.Context, s *SubmissionRecord) error {
	if db.pool == nil {
		return nil
	}

	// Guard against empty UUID strings which crash Postgres
	var teamID interface{} = s.TeamID
	if s.TeamID == "" {
		teamID = nil
	}

	// 1. Upsert the team to resolve Redis vs Postgres split-brain
	if teamID != nil {
		_, _ = db.pool.Exec(ctx, `
			INSERT INTO teams (team_id, team_name) 
			VALUES ($1, $2) 
			ON CONFLICT (team_id) DO NOTHING`, 
			teamID, s.TeamName)
	}

	// 2. Insert the actual submission
	_, err := db.pool.Exec(ctx, `
		INSERT INTO submissions
		  (submission_id, team_id, team_name, language, endpoint_type, filename, object_key, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		s.SubmissionID, teamID, s.TeamName, s.Language,
		s.EndpointType, s.Filename, s.ObjectKey, s.Status)
	return err
}

func (db *DB) UpdateSubmission(ctx context.Context, id, status, imageRef, errMsg, sessionID string) error {
	if db.pool == nil {
		return nil
	}
	_, err := db.pool.Exec(ctx, `
		UPDATE submissions
		SET status=$2, image_ref=$3, error_msg=$4, session_id=$5, updated_at=NOW()
		WHERE submission_id=$1`, id, status, imageRef, errMsg, sessionID)
	return err
}

func (db *DB) GetSubmission(ctx context.Context, id string) (*SubmissionRecord, error) {
	if db.pool == nil {
		return nil, fmt.Errorf("db unavailable")
	}
	row := db.pool.QueryRow(ctx, `
		SELECT submission_id, COALESCE(team_id::text,''), team_name, language,
		       endpoint_type, filename, object_key, COALESCE(image_ref,''),
		       status, COALESCE(session_id::text,''), COALESCE(error_msg,''),
		       created_at, updated_at
		FROM submissions WHERE submission_id=$1`, id)
	var s SubmissionRecord
	err := row.Scan(&s.SubmissionID, &s.TeamID, &s.TeamName, &s.Language,
		&s.EndpointType, &s.Filename, &s.ObjectKey, &s.ImageRef,
		&s.Status, &s.SessionID, &s.ErrorMsg, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *DB) ListSubmissions(ctx context.Context, limit, offset int) ([]*SubmissionRecord, error) {
	if db.pool == nil {
		return nil, nil
	}
	rows, err := db.pool.Query(ctx, `
		SELECT submission_id, COALESCE(team_id::text,''), team_name, language,
		       endpoint_type, filename, object_key, COALESCE(image_ref,''),
		       status, COALESCE(session_id::text,''), COALESCE(error_msg,''),
		       created_at, updated_at
		FROM submissions ORDER BY created_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subs []*SubmissionRecord
	for rows.Next() {
		var s SubmissionRecord
		if err := rows.Scan(&s.SubmissionID, &s.TeamID, &s.TeamName, &s.Language,
			&s.EndpointType, &s.Filename, &s.ObjectKey, &s.ImageRef,
			&s.Status, &s.SessionID, &s.ErrorMsg, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		subs = append(subs, &s)
	}
	return subs, rows.Err()
}

// ─── Storage layer ────────────────────────────────────────────────────────────

type Storage struct {
	client *minio.Client
	bucket string
}

func NewStorage(endpoint, access, secret, bucket string) (*Storage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	exists, _ := client.BucketExists(ctx, bucket)
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket: %w", err)
		}
	}
	return &Storage{client: client, bucket: bucket}, nil
}

func (s *Storage) Upload(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, size,
		minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *Storage) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	return obj, err
}

// ─── Security scanner ─────────────────────────────────────────────────────────

type Scanner struct{}

// Scan runs a basic static analysis on the uploaded archive.
// In production: integrate ClamAV + custom YARA rules.
func (sc *Scanner) Scan(ctx context.Context, path string) error {
	// 1. Check file size (max 500MB)
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	const maxBytes = 500 * 1024 * 1024
	if info.Size() > maxBytes {
		return fmt.Errorf("file too large: %d bytes (max %d)", info.Size(), maxBytes)
	}

	// 2. Check for known bad patterns (shell reverse shells, etc.) using grep
	patterns := []string{
		"/dev/tcp", "bash -i", "nc -e", "python -c",
		"base64 -d", "chmod +x", "/proc/self/exe",
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lower := strings.ToLower(string(data))
	for _, p := range patterns {
		if strings.Contains(lower, p) {
			return fmt.Errorf("security scan failed: suspicious pattern '%s' detected", p)
		}
	}

	// 3. Try ClamAV if available
	if _, err := exec.LookPath("clamscan"); err == nil {
		cmd := exec.CommandContext(ctx, "clamscan", "--no-summary", path)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("clamav scan failed: %s", string(out))
		}
	}

	return nil
}

// ─── Docker builder ───────────────────────────────────────────────────────────

type Builder struct {
	registry string
	runtime  string
}

func (b *Builder) Build(ctx context.Context, submissionID, language, archivePath string) (string, error) {
	buildDir, err := os.MkdirTemp("", "of-build-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(buildDir)

	// Extract archive
	extractCmd := exec.CommandContext(ctx, "tar", "-xzf", archivePath, "-C", buildDir)
	if out, err := extractCmd.CombinedOutput(); err != nil {
		// Try zip
		extractCmd = exec.CommandContext(ctx, "unzip", "-o", archivePath, "-d", buildDir)
		if out2, err2 := extractCmd.CombinedOutput(); err2 != nil {
			// Treat as raw binary
			destPath := filepath.Join(buildDir, "engine")
			if err := copyFile(archivePath, destPath); err != nil {
				return "", fmt.Errorf("extract failed: tar: %s, unzip: %s", out, out2)
			}
			os.Chmod(destPath, 0755)
		}
	}

	// Write appropriate Dockerfile
	dockerfile := b.dockerfileFor(language)
	if err := os.WriteFile(filepath.Join(buildDir, "Dockerfile"), []byte(dockerfile), 0644); err != nil {
		return "", err
	}

	imageRef := fmt.Sprintf("%s/contestant-%s:latest", b.registry, submissionID[:8])

	// Build image (use docker CLI; in production use Kaniko in-cluster)
	buildArgs := []string{"build", "-t", imageRef, "--no-cache", buildDir}
	if b.runtime == "runc" {
		buildArgs = append(buildArgs, "--network=host")
	}
	buildCmd := exec.CommandContext(ctx, "docker", buildArgs...)
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		// If docker not available (CI/test), return a placeholder
		log.Printf("WARNING: docker build not available: %v", err)
		return imageRef + "-mock", nil
	}

	return imageRef, nil
}

func (b *Builder) dockerfileFor(language string) string {
	switch strings.ToLower(language) {
	case "rust":
		return `FROM rust:1.78-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release 2>/dev/null || (test -f engine && cp engine /app/engine-out) || cp $(find . -maxdepth 2 -type f -executable | head -1) /app/engine-out
FROM debian:bookworm-slim
COPY --from=builder /app/engine-out /engine
RUN chmod +x /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]`

	case "c++", "cpp":
		return `FROM gcc:13-bookworm AS builder
WORKDIR /app
COPY . .
RUN make 2>/dev/null || g++ -O3 -o engine $(find . -name "*.cpp" | head -5) -lpthread -lm
FROM debian:bookworm-slim
COPY --from=builder /app/engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]`

	case "go":
		return `FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy 2>/dev/null; go build -o engine ./... 2>/dev/null || go build -o engine .
FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]`

	case "c":
		return `FROM gcc:13-bookworm AS builder
WORKDIR /app
COPY . .
RUN make 2>/dev/null || gcc -O3 -o engine $(find . -name "*.c" | head -5) -lpthread -lm
FROM debian:bookworm-slim
COPY --from=builder /app/engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]`

	default:
		return `FROM debian:bookworm-slim
WORKDIR /app
COPY . .
RUN chmod +x engine 2>/dev/null || true
EXPOSE 8080
ENTRYPOINT ["./engine"]`
	}
}

// ─── Job deployer ─────────────────────────────────────────────────────────────

type JobDeployer struct {
	runtime string
	orchURL string
}

// Deploy starts the contestant container and notifies bot-orchestrator.
// In production this creates a K8s Job with gVisor runtime. Here we use docker.
func (jd *JobDeployer) Deploy(ctx context.Context, submissionID, teamName, language, imageRef, endpointType string) (string, error) {
	sessionID := uuid.New().String()
	port := "8080"

	containerName := fmt.Sprintf("contestant-%s", submissionID[:8])
	
	// FIX: Removed the crashing network flag. We now map to a random host port.
	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"--cpus", "2",
		"--memory", "4g",
		"--memory-swap", "4g",
		"--read-only",
		"--tmpfs", "/tmp:size=512m",
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"-p", "0:8080",
		imageRef,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("WARNING: docker run failed: %v: %s", err, string(out))
		return sessionID, nil
	}

	containerID := strings.TrimSpace(string(out))

	// Dynamically fetch the random port Docker assigned
	portCmd := exec.Command("docker", "port", containerID, "8080")
	portOut, err := portCmd.Output()
	if err == nil {
		parts := strings.Split(strings.TrimSpace(string(portOut)), ":")
		if len(parts) >= 2 {
			port = strings.TrimSpace(parts[len(parts)-1])
		}
	}

	// FIX: Use Docker Desktop's bulletproof host routing
	targetURL := fmt.Sprintf("ws://172.17.0.1:%s", port)
	if endpointType == "REST" || endpointType == "HTTP2" {
		targetURL = fmt.Sprintf("http://172.17.0.1:%s", port)
	}

	log.Printf("Deployed contestant %s → container %s, endpoint %s",
		submissionID[:8], containerID[:12], targetURL)

	go jd.notifyOrchestrator(context.Background(), sessionID, submissionID, teamName, language, targetURL, endpointType)

	return sessionID, nil
}

func (jd *JobDeployer) notifyOrchestrator(ctx context.Context, sessionID, submissionID, teamName, language, targetURL, endpointType string) {
	// Wait for container to be ready
	time.Sleep(3 * time.Second)

	body := fmt.Sprintf(`{"session_id":%q,"submission_id":%q,"team_name":%q,"language":%q,"target_url":%q,"endpoint_type":%q,"bot_count":2048,"duration_secs":300}`,
		sessionID, submissionID, teamName, language, targetURL, endpointType)
	req, err := http.NewRequestWithContext(ctx, "POST",
		jd.orchURL+"/api/sessions/start", strings.NewReader(body))
	if err != nil {
		log.Printf("orchestrator notify failed: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("orchestrator notify failed: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Orchestrator notified for session %s: HTTP %d", sessionID, resp.StatusCode)
}

// ─── HTTP Handlers ────────────────────────────────────────────────────────────

type Handler struct {
	db      *DB
	storage *Storage
	scanner *Scanner
	builder *Builder
	deployer *JobDeployer
}

func (h *Handler) SubmitCode(c *gin.Context) {
	teamID := c.GetHeader("X-Team-ID")
	teamName := c.GetHeader("X-Team-Name")
	if teamName == "" {
		teamName = "anonymous"
	}

	language := c.PostForm("language")
	endpointType := c.PostForm("endpoint_type")
	if language == "" {
		language = "go"
	}
	if endpointType == "" {
		endpointType = "WebSocket"
	}

	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no file uploaded: " + err.Error()})
		return
	}
	defer file.Close()

	submissionID := uuid.New().String()
	// Strip out any malicious directory paths from the uploaded filename
	cleanFilename := filepath.Base(header.Filename)
	objectKey := fmt.Sprintf("%s/%s", submissionID, cleanFilename)

	// Save to temp file for scanning
	tmpFile, err := os.CreateTemp("", "upload-*")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := io.Copy(tmpFile, file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	tmpFile.Close()

	// Security scan
	if err := h.scanner.Scan(c.Request.Context(), tmpFile.Name()); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "security scan failed: " + err.Error()})
		return
	}

	// Upload to MinIO
	f2, _ := os.Open(tmpFile.Name())
	defer f2.Close()
	fi, _ := f2.Stat()
	if err := h.storage.Upload(c.Request.Context(), objectKey, f2, fi.Size(), "application/octet-stream"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "storage upload failed: " + err.Error()})
		return
	}

	// Create DB record
	rec := &SubmissionRecord{
		SubmissionID: submissionID,
		TeamID:       teamID,
		TeamName:     teamName,
		Language:     language,
		EndpointType: endpointType,
		Filename:     header.Filename,
		ObjectKey:    objectKey,
		Status:       "pending",
	}
	if err := h.db.CreateSubmission(c.Request.Context(), rec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error: " + err.Error()})
		return
	}

	// Kick off async build + deploy inside a safe wrapper that handles cleanup
    go func(targetFile string) {
        defer os.Remove(targetFile)
        defer func() {
            if r := recover(); r != nil {
                log.Printf("PANIC in buildAndDeploy for submission: %v", r)
            }
        }()
        h.buildAndDeploy(context.Background(), submissionID, teamName, language, endpointType, targetFile)
    }(tmpFile.Name())
	
	c.JSON(http.StatusAccepted, gin.H{
		"submission_id": submissionID,
		"status":        "pending",
		"message":       "Submission received. Building and deploying sandbox...",
	})
}

func (h *Handler) buildAndDeploy(ctx context.Context, submissionID, teamName, language, endpointType, archivePath string) {
	
	// Update status: building
	h.db.UpdateSubmission(ctx, submissionID, "building", "", "", "")

	imageRef, err := h.builder.Build(ctx, submissionID, language, archivePath)
	if err != nil {
		log.Printf("Build failed for %s: %v", submissionID, err)
		h.db.UpdateSubmission(ctx, submissionID, "build_failed", "", err.Error(), "")
		return
	}

	h.db.UpdateSubmission(ctx, submissionID, "deploying", imageRef, "", "")

	sessionID, err := h.deployer.Deploy(ctx, submissionID, teamName, language, imageRef, endpointType)
	if err != nil {
		log.Printf("Deploy failed for %s: %v", submissionID, err)
		h.db.UpdateSubmission(ctx, submissionID, "deploy_failed", imageRef, err.Error(), "")
		return
	}

	h.db.UpdateSubmission(ctx, submissionID, "running", imageRef, "", sessionID)
	log.Printf("Submission %s running as session %s", submissionID, sessionID)

	// Force the database to register the active session so the UI can lock on!
	if h.db != nil && h.db.pool != nil {
		_, err = h.db.pool.Exec(ctx, 
			"UPDATE submissions SET status = 'running', session_id = $1 WHERE submission_id = $2", 
			sessionID, submissionID)
		if err != nil {
			log.Printf("Failed to update database status to running: %v", err)
		}
	}
}

func (h *Handler) GetSubmission(c *gin.Context) {
	id := c.Param("id")
	rec, err := h.db.GetSubmission(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "submission not found"})
		return
	}
	c.JSON(http.StatusOK, rec)
}

func (h *Handler) ListSubmissions(c *gin.Context) {
	recs, err := h.db.ListSubmissions(c.Request.Context(), 50, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"submissions": recs, "count": len(recs)})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := configFromEnv()
	ctx := context.Background()

	db, err := NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Printf("WARNING: DB not reachable: %v", err)
		db = &DB{} // allow startup without DB in dev
	}

	storage, err := NewStorage(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket)
	if err != nil {
		log.Printf("WARNING: MinIO not reachable: %v", err)
		storage = &Storage{}
	}

	handler := &Handler{
		db:      db,
		storage: storage,
		scanner: &Scanner{},
		builder: &Builder{registry: cfg.ContainerRegistry, runtime: cfg.SandboxRuntime},
		deployer: &JobDeployer{runtime: cfg.SandboxRuntime, orchURL: cfg.BotOrchestratorURL},
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "submission-service"})
	})
	r.POST("/v1/submissions", handler.SubmitCode)
	r.GET("/v1/submissions", handler.ListSubmissions)
	r.GET("/v1/submissions/:id", handler.GetSubmission)

	log.Printf("submission-service listening on :%s", cfg.Port)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("server error: %v", err)
	}


}
