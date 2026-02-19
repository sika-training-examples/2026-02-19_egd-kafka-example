package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/goombaio/namegenerator"
	"github.com/segmentio/kafka-go"
)

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

	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)

	i := 0
	var key string
	var msg string

	for {
		key = strconv.Itoa(i)
		msg = nameGenerator.Generate()
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(key),
			Value: []byte(msg),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}
		fmt.Printf("produce: topic=%s key=%s msg=%s\n", topic, key, msg)
		i++
		time.Sleep(time.Second)
	}
}
