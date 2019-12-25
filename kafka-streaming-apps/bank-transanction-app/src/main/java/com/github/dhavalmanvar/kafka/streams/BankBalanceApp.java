package com.github.dhavalmanvar.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BankBalanceApp {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(BankBalanceApp.class.getName());


        String sourceTopic = "bank-transactions";

        // Enable log compaction for the following two topics
        final String targetTopic = "bank-balance";

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, BankTransactionDTO> source = builder.stream(sourceTopic,
                Consumed.with(Serdes.String(), new CustomSerde<BankTransactionDTO>(BankTransactionDTO.class)));

        final KTable<String, Long> targetTable = source
                .groupByKey()
                .aggregate(
                        () -> 0L,
                        (userName, transaction, total) -> total + transaction.getAmount(),
                        Materialized.with(Serdes.String(), Serdes.Long()));


        targetTable.toStream().to(targetTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

    public static Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

}
