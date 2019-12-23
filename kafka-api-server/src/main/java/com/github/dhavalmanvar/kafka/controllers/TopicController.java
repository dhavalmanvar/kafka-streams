package com.github.dhavalmanvar.kafka.controllers;

import com.github.dhavalmanvar.kafka.dto.TopicInfo;
import com.github.dhavalmanvar.kafka.services.TopicManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("/topic")
public class TopicController {

    private TopicManager topicManager;

    @Autowired
    public TopicController(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public void createTopic(@RequestBody TopicInfo topicInfo) throws Exception {
        this.topicManager.createTopic(topicInfo);
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Set<String> getTopics() throws Exception {
        return topicManager.getTopics();
    }

    @DeleteMapping(value = "/{topic}")
    public void deleteTopic(@PathVariable String topic) throws Exception {
        topicManager.deleteTopic(topic);
    }

}
