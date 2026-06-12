# ==========================================
# STAGE 1: The Builder
# ==========================================
FROM golang:alpine AS builder
WORKDIR /app

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Compile the binary with static linking
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=mod -ldflags="-w -s" -o service .

# ==========================================
# STAGE 2: The Runtime (DinD Sandbox)
# ==========================================
FROM alpine:latest
WORKDIR /app

# 🚀 THE ULTIMATE DNS BYPASS: 
# Instead of downloading via apk, we steal the pre-compiled Docker CLI 
# directly from Docker's official image cache. Zero internet required.
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

# Copy the compiled Go binary from the builder stage
COPY --from=builder /app/service .

# Expose the internal container port
EXPOSE 8080

# Launch the service
CMD ["./service"]