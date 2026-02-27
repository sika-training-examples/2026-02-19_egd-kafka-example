FROM golang:1.25 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build

FROM debian:13-slim
LABEL org.opencontainers.image.source=https://github.com/sika-training-examples/2026-02-19_egd-kafka-example
WORKDIR /app
COPY --from=builder /build/consumer .
CMD ["./consumer"]
