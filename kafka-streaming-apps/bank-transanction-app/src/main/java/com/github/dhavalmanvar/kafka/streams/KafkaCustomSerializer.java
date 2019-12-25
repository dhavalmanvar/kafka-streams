package com.github.dhavalmanvar.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaCustomSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCustomSerializer.class.getName());

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception ex) {
            logger.error("parsing error:", ex);
        }
        return retVal;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

}
