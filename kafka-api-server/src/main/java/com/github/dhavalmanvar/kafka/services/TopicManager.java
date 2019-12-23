package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.dto.TopicInfo;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

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
            KafkaFuture<Void> future = client.createTopics(Collections.singleton(
                    new NewTopic(topicInfo.getTopic(), topicInfo.getPartitions(),
                            topicInfo.getReplicationFactor())),
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

    private AdminClient getAdminClient() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.configManager.getBootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return KafkaAdminClient.create(config);
    }

}
