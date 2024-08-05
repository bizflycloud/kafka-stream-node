package kafka_client

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/getsentry/sentry-go"
	"kafka-stream-node/common"
	"kafka-stream-node/config"
	"kafka-stream-node/logger"
	"strings"
	"time"
)

var (
	Producer sarama.SyncProducer
)

func InitKafkaProducer(conf *config.AppConfig) (sarama.SyncProducer, error) {
	producerConfig := sarama.NewConfig()
	producerConfig.Version = sarama.V2_7_0_0

	if conf.KafkaAuthSubscriber {
		producerConfig.Net.SASL.Enable = true
		producerConfig.Net.SASL.Handshake = true
		producerConfig.Net.SASL.User = conf.KafkaUserSubscriber
		producerConfig.Net.SASL.Password = conf.KafkaPasswordSubscriber

		switch conf.KafkaMechanismSubscriber {
		case "SHA512":
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA512}
			}
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		case "SHA256":
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA256}
			}
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		default:
			sentry.CaptureException(fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismSubscriber))
			logger.Fatalf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismSubscriber)
		}
	}

	producerConfig.Net.TLS.Enable = false
	producerConfig.Producer.Return.Successes = true

	brokerList := strings.Split(conf.KafkaBrokerPublisher, ",")
	producer, err := sarama.NewSyncProducer(brokerList, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return producer, nil
}

func BatchAndPushMessages(messageChan chan *sarama.ProducerMessage) {
	var messageBuffer []*sarama.ProducerMessage
	// convert batch time from second to nanosecond
	batchTime := config.Instance.BatchTimeMessagePush * 1000000000
	ticker := time.NewTicker(time.Duration(batchTime))
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				// The channel has been closed and all messages have been processed
				if len(messageBuffer) > 0 {
					pushMessages(messageBuffer)
				}
				return
			}
			messageBuffer = append(messageBuffer, msg)
			// Batch zise message push to subscriber
			if len(messageBuffer) >= config.Instance.BatchSizeMessagePush {
				pushMessages(messageBuffer)
				messageBuffer = nil
			}
		case <-ticker.C:
			// When the bathTime is over and the channel does not have enough messages, the message will also be pushed.
			if len(messageBuffer) > 0 {
				pushMessages(messageBuffer)
				messageBuffer = nil
			}
		}
	}
}

func pushMessages(messages []*sarama.ProducerMessage) {
	err := Producer.SendMessages(messages)
	if err != nil {
		logger.Errorf("Failed to send messages: %v", err)
	} else {
		logger.Infof("Successfully sent %d messages", len(messages))
	}
}
