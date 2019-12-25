package com.github.dhavalmanvar.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaCustomDeserializer<T> implements Deserializer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCustomDeserializer.class.getName());

    private Class <T> type;

    private ObjectMapper mapper = new ObjectMapper();

    public KafkaCustomDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {

            logger.error(e.getMessage());
        }
        return obj;
    }

    @Override
    public void close() {

    }
}
