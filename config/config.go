package config

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
	"sync"
)

type AppConfig struct {
	Env string `mapstructure:"env"`

	KafkaBrokerPublisher    string `mapstructure:"Kafka_broker_publisher"`
	KafkaUserPublisher      string `mapstructure:"Kafka_user_publisher"`
	KafkaPasswordPublisher  string `mapstructure:"Kafka_password_publisher"`
	KafkaMechanismPublisher string `mapstructure:"kafka_mechanism_publisher"`
	KafkaTopicPublisher     string `mapstructure:"Kafka_topic_publisher"`
	KafkaAuthPublisher      bool   `mapstructure:"Kafka_auth_publisher"`

	KafkaBrokerSubscriber    string `mapstructure:"Kafka_broker_subscriber"`
	KafkaUserSubscriber      string `mapstructure:"Kafka_user_subscriber"`
	KafkaPasswordSubscriber  string `mapstructure:"Kafka_password_subscriber"`
	KafkaMechanismSubscriber string `mapstructure:"kafka_mechanism_subscriber"`
	KafkaTopicSubscriber     string `mapstructure:"Kafka_topic_subscriber"`
	KafkaAuthSubscriber      bool   `mapstructure:"Kafka_auth_subscriber"`

	BatchSizeMessagePush int   `mapstructure:"Batch_size_message_push"`
	BatchTimeMessagePush int64 `mapstructure:"Batch_time_message_push"`

	SentryDSN string `mapstructure:"sentry_dsn"`
}

var (
	Instance *AppConfig = nil
	once     sync.Once
)

func getEnv(key string, fallback interface{}) interface{} {
	var rValue interface{}
	value, exists := os.LookupEnv(key)
	if !exists {
		rValue = fallback
	} else {
		rValue = value
	}
	return rValue
}

func InitConfig() *AppConfig {
	// synchronize Instance config
	once.Do(
		func() {
			var err error
			// load .env config
			err = godotenv.Load(".env")
			if err != nil {
				log.Println("Error: ", err)
			}
			KafkaAuthPublisher, _ := strconv.ParseBool(getEnv("KAFKA_AUTH_PUBLISHER", "false").(string))
			KafkaAuthSubscriber, _ := strconv.ParseBool(getEnv("KAFKA_AUTH_PUBLISHER_SUBSCRIBER", "false").(string))
			BatchSizeMessagePush, _ := strconv.ParseInt(getEnv("BATCH_SIZE_MESSAGE_PUSH", "500").(string), 10, 64)
			BatchTimeMessagePush, _ := strconv.ParseInt(getEnv("BATCH_TIME_MESSAGE_PUSH", "5").(string), 10, 64)
			Instance = &AppConfig{
				Env:                     getEnv("ENV", "").(string),
				KafkaBrokerPublisher:    getEnv("KAFKA_BROKER_PUBLISHER", "").(string),
				KafkaUserPublisher:      getEnv("KAFKA_USER_PUBLISHER", "").(string),
				KafkaPasswordPublisher:  getEnv("KAFKA_PASSWORD_PUBLISHER", "").(string),
				KafkaTopicPublisher:     getEnv("KAFKA_TOPIC_PUBLISHER", "").(string),
				KafkaMechanismPublisher: getEnv("KAFKA_MECHANISM_PUBLISHER", "").(string),
				KafkaAuthPublisher:      KafkaAuthPublisher,

				KafkaBrokerSubscriber:    getEnv("KAFKA_BROKER_SUBSCRIBER", "").(string),
				KafkaUserSubscriber:      getEnv("KAFKA_USER_SUBSCRIBER", "").(string),
				KafkaPasswordSubscriber:  getEnv("KAFKA_PASSWORD_SUBSCRIBER", "").(string),
				KafkaTopicSubscriber:     getEnv("KAFKA_TOPIC_SUBSCRIBER", "").(string),
				KafkaMechanismSubscriber: getEnv("KAFKA_MECHANISM_SUBSCRIBER", "").(string),
				KafkaAuthSubscriber:      KafkaAuthSubscriber,

				BatchSizeMessagePush: int(BatchSizeMessagePush),
				BatchTimeMessagePush: BatchTimeMessagePush,

				SentryDSN: getEnv("SENTRY_DSN", "").(string),
			}
		},
	)

	return Instance
}
