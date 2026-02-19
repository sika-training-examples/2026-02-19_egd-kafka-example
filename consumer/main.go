package main

import (
	"context"
	"fmt"
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
		if err != nil {
			panic("could not read message " + err.Error())
		}
		fmt.Printf("consume: topic=%s key=%s msg=%s\n", topic, msg.Key, msg.Value)
	}
}
