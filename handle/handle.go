package handle

import (
	"github.com/IBM/sarama"
	"kafka-stream-node/config"
	"kafka-stream-node/logger"
)

var MessageChan = make(chan *sarama.ProducerMessage, config.Instance.BatchSizeMessagePush)

func HandleData(valueMessage []byte) {
	logger.Info("value message: ", string(valueMessage))
	// push message into channel
	MessageChan <- &sarama.ProducerMessage{Topic: config.Instance.KafkaTopicSubscriber, Value: sarama.StringEncoder(valueMessage)}
}
