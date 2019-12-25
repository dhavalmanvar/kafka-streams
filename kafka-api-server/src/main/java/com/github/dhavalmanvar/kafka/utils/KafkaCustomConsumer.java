package com.github.dhavalmanvar.kafka.utils;

import com.github.dhavalmanvar.kafka.dto.BankAccountDTO;
import com.github.dhavalmanvar.kafka.dto.BankTransactionDTO;
import com.github.dhavalmanvar.kafka.serializer.KafkaCustomDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaCustomConsumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KafkaProducerUtil.class.getName());
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Consumer<String, BankTransactionDTO> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
                new KafkaCustomDeserializer<BankTransactionDTO>(BankTransactionDTO.class));
        consumer.subscribe(Collections.singletonList("bank-transactions"));

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        while (true) {
            try {
                ConsumerRecords<String, BankTransactionDTO> records = consumer.poll(Duration.ofMillis(500));
                if(records != null && !records.isEmpty()) {
                    records.forEach(record -> System.out.println(record.key() + " --> " + record.value()));
                }
            } catch (Exception ex) {
                logger.error("Exception: ", ex);
                break;
            }
        }
    }

}
