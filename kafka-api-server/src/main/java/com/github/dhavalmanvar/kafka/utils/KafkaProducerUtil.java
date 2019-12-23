package com.github.dhavalmanvar.kafka.utils;

import com.github.dhavalmanvar.kafka.dto.MessageDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaProducerUtil {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerUtil.class.getName());

    public static void main(String[] args) {

    }

    public static void produceMessage(final MessageDTO message, final KafkaProducer producer) {
        ProducerRecord record = getProducerRecord(message);
        producer.send(record, (metadata, exception) -> {
            if(exception != null) {
                logger.error("KafkaProducerUtil.produceMessage1", exception);
            }
        });
    }

    public static void produceMessage(final List<MessageDTO> messages, final String bootstrapServers,
                                      final String producerId) {
        KafkaProducer producer = buildKafkaProducer(messages.get(0), bootstrapServers, producerId);

        try {
            messages.forEach(message -> {
                ProducerRecord record = getProducerRecord(message);
                producer.send(record, (metadata, exception) -> {
                    if(exception != null) {
                        logger.error("KafkaProducerUtil.produceMessage1", exception);
                    }
                });
            });
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void produceMessage(final MessageDTO message, final String bootstrapServers,
                                      final String producerId) {
        KafkaProducer producer = buildKafkaProducer(message, bootstrapServers, producerId);
        try {
            ProducerRecord record = getProducerRecord(message);
            producer.send(record, (metadata, exception) -> {
                if(exception != null) {
                    logger.error("KafkaProducerUtil.produceMessage1", exception);
                }
            });
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static KafkaProducer buildKafkaProducer(final MessageDTO message,
                                                   final String bootstrapServers,
                                                   final String producerId) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                getSerializerClassName(message.getKeyType()));
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                getSerializerClassName(message.getValueType()));

        return new KafkaProducer(config);
    }

    public static ProducerRecord getProducerRecord(final MessageDTO message) {
        ProducerRecord record = null;

        if(message.getPartition() != null) {
            record = new ProducerRecord(message.getTopic(), message.getPartition(),
                    message.getKey(), message.getValue());
        } else {
            record = new ProducerRecord(message.getTopic(),
                    message.getKey(), message.getValue());
        }
        return record;
    }

    public static String getSerializerClassName(String type) {
        if(type == null || type.trim().isEmpty()) {
            type = "string";
        }

        if("string".equalsIgnoreCase(type)) {
            return StringSerializer.class.getName();
        } else if("int".equalsIgnoreCase(type)) {
            return IntegerSerializer.class.getName();
        } else if("long".equalsIgnoreCase(type)) {
            return LongSerializer.class.getName();
        } else if("float".equalsIgnoreCase(type)) {
            return FloatSerializer.class.getName();
        }

        throw new RuntimeException("Type " + type + " is not supported.");
    }

}
