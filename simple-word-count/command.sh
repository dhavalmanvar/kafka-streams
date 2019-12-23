# Create input topic
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic word-count-input --replication-factor 1 --partitions 2

# Create output topic
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic word-count-output --replication-factor 1 --partitions 2

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                                --topic word-count-output \
                                --from-beginning \
                                --formatter kafka.tools.DefaultMessageFormatter \
                                --property print.key=true \
                                --property print.value=true \
                                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
                                --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


