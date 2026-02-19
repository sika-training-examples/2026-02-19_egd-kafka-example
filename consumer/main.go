package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	brokerAddress := os.Getenv("KAFKA_BROKER_ADDRESS")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP")
	consume(ctx, brokerAddress, topic, groupID)
}

func consume(
	ctx context.Context,
	brokerAddr string,
	topic string,
	groupID string,
) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topic,
		GroupID: groupID,
	})
	for {
		msg, err := r.ReadMessage(ctx)
		handleErr(err)
		fmt.Printf(
			"[%s]->(this) key=%s msg=%s\n",
			topic, string(msg.Key), string(msg.Value),
		)
	}
}

func handleErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
