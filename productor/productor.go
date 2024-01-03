package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	broker1 = "localhost:9092"
	broker2 = "localhost:9094"
	topic   = "test"
)

func main() {
	producer()
}

func producer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{broker1, broker2}, config)
	if err != nil {
		log.Fatalln("Failed to create producer:", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln("Failed to close producer:", err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	for i := 0; i < 10; i++ {
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("Failed to produce message:", err)
		} else {
			fmt.Printf("Produced message to partition %d at offset %d\n", partition, offset)
		}
		time.Sleep(time.Second * 1)
	}

}
