package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	Version  int    `json:"v"`
	Hostname string `json:"h"`
	I        int    `json:"i"`
	Message  string `json:"m"`
}

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

	hostname, err := os.Hostname()
	handleErr(err)

	for {
		sourceKafkaMsg, err := r.ReadMessage(ctx)
		handleErr(err)

		sourceMsg, err := deserialize(sourceKafkaMsg.Value)
		if err != nil {
			log.Printf("error deserializing message: %v", err)
			continue
		}
		targetMsg := serialize(Message{
			Version:  1,
			Hostname: hostname,
			I:        sourceMsg.I,
			Message:  "Hello " + sourceMsg.Message,
		})

		err = w.WriteMessages(ctx, kafka.Message{
			Key:   sourceKafkaMsg.Key,
			Value: []byte(targetMsg),
		})
		handleErr(err)

		fmt.Printf(
			"[%s]->(this)->[%s] key=%s msg=%s\n",
			sourceTopic, targetTopic, string(sourceKafkaMsg.Key), targetMsg,
		)
		time.Sleep(time.Second)
	}
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func handleErrLogOnly(err error) {
	if err != nil {
		log.Printf("error: %v", err)
	}
}

func serialize(msg Message) string {
	jsonBytes, err := json.Marshal(msg)
	handleErr(err)
	return string(jsonBytes)
}

func deserialize(data []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return Message{}, err
	}
	return msg, nil
}
