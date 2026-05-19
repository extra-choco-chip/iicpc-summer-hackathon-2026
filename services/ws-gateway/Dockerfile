FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates

# Use Go module proxy with fallback to direct
ENV GOPROXY=https://proxy.golang.org,direct
ENV GONOSUMDB=*
ENV GOFLAGS=-mod=mod

WORKDIR /app

COPY go.mod ./
RUN go mod download all

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=mod -ldflags="-w -s" -o service .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/service ./service
EXPOSE 8080
ENTRYPOINT ["./service"]
