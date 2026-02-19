FROM golang:1.25 as builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build

FROM debian:13-slim
WORKDIR /app
COPY --from=builder /build/processor .
CMD ["./processor"]
