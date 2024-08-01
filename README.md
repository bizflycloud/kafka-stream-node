# Kafka Stream Node

Kafka Stream Node is a Go application built to act as a Kafka consumer. This application reads data from a Kafka topic and pushes that data into another Kafka topic. All necessary configurations are taken from the .env file.

## System Requirements

- Docker
- Go 1.18 or later

## Installation

### 1. Clone repository

```sh
git clone https://github.com/bizflycloud/kafka-stream-node
cd kafka-stream-node
```
### 2. Create .env file in root folder
```
#example content 
ENV=develop
# Publisher
# Kafka broker address for Publisher (Kafka broker cluster to push data)
KAFKA_BROKER_PUBLISHER=

# Authentication configuration for Publisher (true if authentication is required, false if not)
KAFKA_AUTH_PUBLISHER=

# Username for Publisher (used if KAFKA_AUTH_PUBLISHER is true)
KAFKA_USER_PUBLISHER=

# Password for Publisher (used if KAFKA_AUTH_PUBLISHER is true)
KAFKA_PASSWORD_PUBLISHER=

# Kafka topic name for Publisher to read data from
KAFKA_TOPIC_PUBLISHER=

# Kafka scram mechanism type for Publisher (used if KAFKA_AUTH_PUBLISHER is true, accepts values "SHA256" or "SHA512")
KAFKA_MECHANISM_PUBLISHER=


# Subscriber
# Kafka broker address for Subscriber (Kafka broker cluster to read data)
KAFKA_BROKER_SUBSCRIBER=

# Authentication configuration for Subscriber (true if authentication is required, false if not)
KAFKA_AUTH_PUBLISHER_SUBSCRIBER=false

# Kafka topic name for Subscriber to send data to
KAFKA_TOPIC_SUBSCRIBER=

# Username for Subscriber (used if KAFKA_AUTH_PUBLISHER_SUBSCRIBER is true)
KAFKA_USER_SUBSCRIBER=

# Password for Subscriber (used if KAFKA_AUTH_PUBLISHER_SUBSCRIBER is true)
KAFKA_PASSWORD_SUBSCRIBER=

# Kafka scram mechanism type for Subscriber (used if KAFKA_AUTH_SUBSCRIBER is true, accepts values "SHA256" or "SHA512")
KAFKA_MECHANISM_SUBSCRIBER="SHA256"
```
### 3. Run project 
* docker-compose
```sh
sudo docker-compose up
```

* docker build
```sh
sudo docker build -t kafka-stream-node .
sudo docker run --network kafka-network --name kafka-stream-node  kafka-stream-node
```

* in terminal 
```sh
go run main.go
```
