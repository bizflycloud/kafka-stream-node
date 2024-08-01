package kafka_target

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/getsentry/sentry-go"
	"github.com/xdg-go/scram"
	"service_bus_consumer/config"
	"service_bus_consumer/logger"
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

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
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if conf.KafkaMechanismSubscriber == "SHA256" {
			producerConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
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
