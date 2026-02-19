package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	brokerAddress := os.Getenv("KAFKA_BROKER_ADDRESS")
	sourceTopic := os.Getenv("KAFKA_SOURCE_TOPIC")
	sourceGroup := os.Getenv("KAFKA_SOURCE_GROUP")
	targerTopic := os.Getenv("KAFKA_TARGET_TOPIC")
	process(ctx, brokerAddress, sourceTopic, sourceGroup, targerTopic)
}

func process(
	ctx context.Context,
	brokerAddr string,
	sourceTopic string,
	sourceGroup string,
	targetTopic string,
) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   sourceTopic,
		GroupID: sourceGroup,
	})
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddr},
		Topic:   targetTopic,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message " + err.Error())
		}
		err = w.WriteMessages(ctx, kafka.Message{
			Key:   msg.Key,
			Value: []byte("Hello " + string(msg.Value)),
		})
		if err != nil {
			log.Printf("could not write message " + err.Error())
		}
		fmt.Printf(
			"process: source_topic=%s target_topic=%s key=%s source_msg=%s\n",
			sourceTopic, targetTopic, string(msg.Key), string(msg.Value),
		)
		time.Sleep(time.Second)
	}
}
