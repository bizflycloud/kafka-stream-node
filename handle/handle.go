package handle

import (
	"github.com/IBM/sarama"
	"service_bus_consumer/config"
	"service_bus_consumer/logger"
	"time"
)

var (
	Producer sarama.SyncProducer
)
var MessageChan = make(chan *sarama.ProducerMessage, config.Instance.BatchSizeMessagePush)

func HandleData(valueMessage []byte) {
	logger.Info("value message: ", string(valueMessage))
	// push message into channel
	MessageChan <- &sarama.ProducerMessage{Topic: config.Instance.KafkaTopicSubscriber, Value: sarama.StringEncoder(valueMessage)}
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
