package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
)

const (
	broker1 = "localhost:9092"
	broker2 = "localhost:9094"
	topic   = "test"
)

func main() {
	consumer()
}
func consumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{broker1, broker2}, config)
	if err != nil {
		log.Fatalln("Failed to create consumer:", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Failed to close consumer:", err)
		}
	}()

	offset := sarama.OffsetNewest
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, offset)
	if err != nil {
		log.Fatalln("Failed to create partition consumer:", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer wg.Done()
		for {
			select {
			case msg, ok := <-partitionConsumer.Messages():
				if !ok {
					return
				}
				// 手动提交Offset
				fmt.Printf("Consumed message offset %d: %s\n", msg.Offset, msg.Value)
			case err, ok := <-partitionConsumer.Errors():
				if !ok {
					return
				}
				log.Println("Error:", err)
			case <-done:
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	select {
	case <-sigterm:
		log.Println("Interrupted")
		partitionConsumer.AsyncClose()
	}
	wg.Wait()
}
