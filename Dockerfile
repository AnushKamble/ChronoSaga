# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git make

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/chronosaga ./cmd/chronosaga

# Runtime Stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata curl

WORKDIR /app
COPY --from=builder /bin/chronosaga /usr/local/bin/chronosaga
COPY migrations ./migrations

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/chronosaga"]
CMD ["api"]
