package com.github.dhavalmanvar.kafka.streams.bank;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {

    public static final String topic = "bank-transactions";

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-transaction-producer");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<>(config);

        int batch = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            System.out.println("Producing transaction batch: " + batch);
            try {
                producer.send(nextTransaction("dhaval"));
                Thread.sleep(100);
                producer.send(nextTransaction("amit"));
                Thread.sleep(100);
                producer.send(nextTransaction("nimit"));
                Thread.sleep(100);
                batch++;
            } catch (InterruptedException ex) {
                break;
            }
        }
    }

    public static ProducerRecord<String, String> nextTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        Integer amount = ThreadLocalRandom.current().nextInt(100);
        Instant time = Instant.now();

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", time.toString());
        return new ProducerRecord<>(topic,name,transaction.toString());
    }
}
