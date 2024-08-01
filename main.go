package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/getsentry/sentry-go"
	"log"
	"net/http"
	"os"
	"os/signal"
	"service_bus_consumer/common"
	"service_bus_consumer/config"
	"service_bus_consumer/handle"
	"service_bus_consumer/kafka_target"
	"service_bus_consumer/logger"
	"sync"
	"syscall"
	"time"
)

type Consumer struct {
	ready         chan bool
	ctx           context.Context
	tracingClient *http.Client
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				logger.Error("message channel was closed")
				return nil
			} else {
				handle.HandleData(message.Value)
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

func main() {
	// init kafka subscriber
	var err error
	handle.Producer, err = kafka_target.InitKafkaProducer()
	if err != nil {
		return
	}
	if err != nil {
		logger.Error("connect kafka subscriber: ", err.Error())
		return
	}
	conf := config.InitConfig()
	brokers := []string{conf.KafkaBrokerPublisher}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_7_0_0
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	if conf.KafkaAuthPublisher {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Handshake = true
		kafkaConfig.Net.TLS.Enable = false
		kafkaConfig.Net.SASL.User = conf.KafkaUserPublisher
		kafkaConfig.Net.SASL.Password = conf.KafkaPasswordPublisher
		if conf.KafkaMechanismPublisher == "SHA512" {
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA512} }
			kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if conf.KafkaMechanismPublisher == "SHA256" {
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA256} }
			kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else {
			sentry.CaptureException(fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismPublisher))
			logger.Fatalf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismPublisher)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{
		ready: make(chan bool),
		ctx:   ctx,
	}
	consumerGroup, err := sarama.NewConsumerGroup(brokers, fmt.Sprintf("%v-group-%v", conf.KafkaTopicPublisher, conf.Env), kafkaConfig)
	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{conf.KafkaTopicPublisher}, &consumer); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("Terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("Terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(consumerGroup, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()

	log.Println("Waiting for the consumer group to finish processing...")
	<-time.After(10 * time.Second)
	log.Println("Shutdown complete")
}
