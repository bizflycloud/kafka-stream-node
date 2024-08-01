package handle

import (
	"github.com/IBM/sarama"
	"service_bus_consumer/config"
	"service_bus_consumer/logger"
)

var (
	Producer sarama.SyncProducer
)

func HandleData(valueMessage []byte) {
	logger.Info("value message: ", string(valueMessage))
	msg := &sarama.ProducerMessage{
		Topic: config.Instance.KafkaTopicSubscriber,
		Value: sarama.StringEncoder(valueMessage),
	}
	// send to topic subscriber
	_, _, err := Producer.SendMessage(msg)
	if err != nil {
		logger.Error("producer can't send message: ", err.Error())
	}
}
