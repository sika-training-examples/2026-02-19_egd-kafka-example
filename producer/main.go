package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	Version  int    `json:"version"`
	Hostname string `json:"hostname"`
	I        int    `json:"i"`
	Message  string `json:"msg"`
}

func main() {
	ctx := context.Background()
	brokerAddress := os.Getenv("KAFKA_BROKER_ADDRESS")
	topic := os.Getenv("KAFKA_TOPIC")
	produce(ctx, brokerAddress, topic)
}

func produce(
	ctx context.Context,
	brokerAddr string,
	topic string,
) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddr},
		Topic:   topic,
	})

	i := 0
	hostname, err := os.Hostname()
	handleErr(err)

	for {
		key := strconv.Itoa(i)
		msg := serialize(Message{
			Version:  1,
			I:        i,
			Hostname: hostname,
			Message:  randomKind() + " " + randomName(),
		})
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(key),
			Value: []byte(msg),
		})
		handleErr(err)
		fmt.Printf("(this)->[%s] key=%s msg=%s\n", topic, key, msg)
		i++
		time.Sleep(time.Second)
	}
}

func randomKind() string {
	kinds := []string{"dog", "cat", "rat", "bat", "eel"}
	return kinds[rand.Intn(len(kinds))]
}

func randomName() string {
	names := []string{
		"Dela", "Nela", "Fred", "Debie", "Kuna",
	}
	return names[rand.Intn(len(names))]
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func serialize(msg Message) string {
	jsonBytes, err := json.Marshal(msg)
	handleErr(err)
	return string(jsonBytes)
}
