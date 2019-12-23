package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.dto.MessageDTO;
import com.github.dhavalmanvar.kafka.dto.ProducerDTO;
import com.github.dhavalmanvar.kafka.utils.KafkaProducerUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceHelper extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ProducerServiceHelper.class.getName());

    private String bootstrapServers;

    private ProducerDTO producerDTO;

    private AtomicBoolean updating = new AtomicBoolean(false);

    private AtomicBoolean terminated = new AtomicBoolean(false);

    public ProducerServiceHelper(ProducerDTO producerDTO, String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producerDTO = producerDTO;
    }

    public ProducerDTO getProducerDTO() {
        return producerDTO;
    }

    @Override
    public void run() {
        List<MessageDTO> messages = producerDTO.getMessages();
        KafkaProducer producer =
                KafkaProducerUtil.buildKafkaProducer(messages.get(0), bootstrapServers, producerDTO.getProducerId());
        long messageCount = 0;
        Random random = new Random();
        while(!terminated.get()) {
            if(!updating.get()) {
                try {
                    messages = producerDTO.getMessages();
                    MessageDTO messageDTO = messages.get(random.nextInt(messages.size()));
                    KafkaProducerUtil.produceMessage(messageDTO, producer);
                    if(producerDTO.getFrequency() > 499) {
                        producer.flush();
                    }
                    messageCount++;
                    Thread.sleep(producerDTO.getFrequency());
                } catch (Exception ex) {
                    logger.error("ProducerServiceHelper.run: " + producerDTO.getProducerId(), ex);
                }
            }
        }
        producer.flush();
        producer.close();
        logger.info("Producer " + producerDTO.getProducerId() + " has been terminated successfully. " +
                "It has produced " + messageCount + " records." );
    }

    public void updateProducer(ProducerDTO producerDTO) {
        this.updating.set(true);
        this.producerDTO = producerDTO;
        this.updating.set(false);
        this.interrupt();
    }

    public void terminate() {
        this.terminated.set(true);
        this.interrupt();
    }

}
