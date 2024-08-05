package main

import (
	"kafka-stream-node/config"
	"kafka-stream-node/handle"
	"kafka-stream-node/kafka_client"
	"kafka-stream-node/logger"
)

func main() {
	var err error
	conf := config.InitConfig()

	// init kafka_client producer
	kafka_client.Producer, err = kafka_client.InitKafkaProducer(conf)
	if err != nil {
		logger.Error("connect kafka subscriber: ", err.Error())
		return
	}

	// run goroutine push messages
	go kafka_client.BatchAndPushMessages(handle.MessageChan)

	// run kafka_client publisher to read messages
	kafka_client.KafkaPushlisher(conf)
}
