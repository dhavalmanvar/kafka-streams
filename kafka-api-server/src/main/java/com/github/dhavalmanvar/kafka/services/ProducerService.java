package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.dto.MessageDTO;
import com.github.dhavalmanvar.kafka.dto.ProducerDTO;
import com.github.dhavalmanvar.kafka.utils.KafkaProducerUtil;
import org.apache.kafka.common.protocol.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProducerService {

    private Map<String, ProducerServiceHelper> producers;

    private ConfigManager configManager;

    @Autowired
    public ProducerService(ConfigManager configManager) {
        producers = new HashMap<>();
        this.configManager = configManager;
    }

    public void produceMessage(final MessageDTO messageDTO) {
        KafkaProducerUtil.produceMessage(messageDTO, configManager.getBootstrapServers(), "common");
    }

    public void updateProducer(ProducerDTO producerDTO) {
        ProducerServiceHelper helper = producers.get(producerDTO.getProducerId());
        if (helper == null) {
            helper = new ProducerServiceHelper(producerDTO, configManager.getBootstrapServers());
            producers.put(producerDTO.getProducerId(), helper);
            helper.start();
        } else {
            helper.updateProducer(producerDTO);
        }
    }

    public List<ProducerDTO> getProducers() {
        List<ProducerDTO> result = new ArrayList<>();
        producers.forEach((key, value) -> result.add(value.getProducerDTO()));
        return result;
    }

    public void deleteProducer(String producerId) {
        ProducerServiceHelper helper = producers.remove(producerId);
        if(helper != null) {
            helper.terminate();
        }
    }

}
