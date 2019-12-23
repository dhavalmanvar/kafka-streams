package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.dto.TopicInfo;
import kafka.log.LogConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class TopicManager {

    private ConfigManager configManager;

    @Autowired
    public TopicManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    public void createTopic(TopicInfo topicInfo) throws Exception {
        AdminClient client = getAdminClient();
        try {
            final Map<String, String> props = new HashMap<>();
            if(topicInfo.getLogCompact()) {
                props.put(LogConfig.CleanupPolicyProp(), LogConfig.Compact());
                props.put(LogConfig.RetentionMsProp(), "100");
            }
            KafkaFuture<Void> future = client.createTopics(Collections.singleton(
                    new NewTopic(topicInfo.getTopic(), topicInfo.getPartitions(),
                            topicInfo.getReplicationFactor()).configs(props)),
                            new CreateTopicsOptions().timeoutMs(5000)).all();
            future.get();
        } finally {
            client.close();
        }
    }

    public Set<String> getTopics() throws Exception {
        AdminClient client = getAdminClient();
        try {
            ListTopicsResult ltr = client.listTopics();
            return ltr.names().get();
        } finally {
            client.close();
        }
    }

    public void deleteTopic(String topic) throws Exception {
        AdminClient client = getAdminClient();
        KafkaFuture<Void> future = client.deleteTopics(Collections.singleton(topic)).all();
        try {
            future.get();
        } finally {
            client.close();
        }
    }

    public void deleteAllTopic() throws Exception {
        AdminClient client = getAdminClient();
        KafkaFuture<Void> future = client.deleteTopics(getTopics()).all();
        try {
            future.get();
        } finally {
            client.close();
        }
    }

    private AdminClient getAdminClient() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.configManager.getBootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return KafkaAdminClient.create(config);
    }

}
