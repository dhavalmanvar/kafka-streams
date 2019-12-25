package com.github.dhavalmanvar.kafka.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerde<T> implements Serde {

    private Class type;

    public CustomSerde(Class type) {
        this.type = type;
    }


    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new KafkaCustomSerializer();
    }

    @Override
    public Deserializer deserializer() {
        return new KafkaCustomDeserializer(this.type);
    }
}
