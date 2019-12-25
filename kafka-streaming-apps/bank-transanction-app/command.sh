#!/bin/bash

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic bank-balance --partitions 1 --replication-factor 1 --config cleanup.policy=compact

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic bank-transactions --partitions 1 --replication-factor 1

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank-transactions --from-beginning --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank-balance --from-beginning --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer