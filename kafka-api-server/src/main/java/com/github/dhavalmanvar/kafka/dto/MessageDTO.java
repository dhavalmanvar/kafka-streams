package com.github.dhavalmanvar.kafka.dto;

import com.github.dhavalmanvar.kafka.DataType;

import javax.xml.crypto.Data;
import java.io.Serializable;

public class MessageDTO implements Serializable {

    private String topic;

    private Integer partition;

    private DataType keyType;

    private DataType valueType;

    private Object key;

    private Object value;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public void setKeyType(DataType keyType) {
        this.keyType = keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    public void setValueType(DataType valueType) {
        this.valueType = valueType;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
