package com.github.dhavalmanvar.kafka.dto;

import java.io.Serializable;

public class TopicInfo implements Serializable {

    private String topic;

    private Short replicationFactor;

    private Integer partitions;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }
}
