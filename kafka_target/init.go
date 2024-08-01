package kafka_target

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/getsentry/sentry-go"
	"service_bus_consumer/common"
	"service_bus_consumer/config"
	"service_bus_consumer/logger"
)

func InitKafkaProducer() (sarama.SyncProducer, error) {
	conf := config.InitConfig()
	producerConfig := sarama.NewConfig()
	producerConfig.Version = sarama.V2_7_0_0
	if conf.KafkaAuthSubscriber {
		producerConfig.Net.SASL.Enable = true
		producerConfig.Net.SASL.Handshake = true
		producerConfig.Net.SASL.User = conf.KafkaUserSubscriber
		producerConfig.Net.SASL.Password = conf.KafkaPasswordSubscriber
		if conf.KafkaMechanismSubscriber == "SHA512" {
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA512} }
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if conf.KafkaMechanismSubscriber == "SHA256" {
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &common.XDGSCRAMClient{HashGeneratorFcn: common.SHA256} }
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else {
			sentry.CaptureException(fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismPublisher))
			logger.Fatalf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", conf.KafkaMechanismPublisher)
		}
	}
	producerConfig.Net.TLS.Enable = false
	producerConfig.Producer.Return.Successes = true

	brokerList := []string{conf.KafkaBrokerSubscriber}
	producer, err := sarama.NewSyncProducer(brokerList, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err.Error())
	}

	return producer, nil
}
