package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092", "localhost:9094"} // 替换为您的Kafka集群的实际地址
	//brokers := []string{"192.168.2.124:9092", "192.168.2.124:9094"} // 替换为您的Kafka集群的实际地址

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// 创建一个Kafka生产者
	config.Producer.Return.Successes = true // 将此行添加到您的代码中
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Error closing Kafka producer: %v", err)
		}
	}()

	// 创建一个Kafka消费者
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing Kafka consumer: %v", err)
		}
	}()

	// 检查Kafka集群的健康状态
	checkHealth(brokers)

	// 在这里编写您的Kafka测试逻辑
	// ...

	// 等待Ctrl+C来关闭程序
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
}

func checkHealth(brokers []string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka cluster admin: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("Error closing Kafka cluster admin: %v", err)
		}
	}()

	// 获取Kafka集群的健康状态
	brokersList, controllerID, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("Error describing Kafka cluster: %v", err)
	}

	fmt.Println("Kafka Cluster Health:")
	fmt.Printf("  Broker Count: %d\n", len(brokersList))
	fmt.Printf("  Controller ID: %d\n", controllerID)
}
