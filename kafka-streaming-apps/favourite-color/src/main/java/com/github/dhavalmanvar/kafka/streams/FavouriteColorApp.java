package com.github.dhavalmanvar.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColorApp {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(FavouriteColorApp.class.getName());


        String sourceTopic = "favourite-color-src";

        // Enable log compaction for the following two topics
        final String interimTopic = "favourite-color-int";
        final String targetTopic = "favourite-color-target";

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source = builder.stream(sourceTopic);

        final List<String> validColors = Arrays.asList("green", "red", "blue");

        source.filter((key, value) -> value.split(",").length == 2)
                .map((key, value) -> new KeyValue<>(value.split(",")[0].toLowerCase(), value.split(",")[1].toLowerCase()))
                .filter((user, color) -> validColors.contains(color))
                .to(interimTopic);

        final KTable<String, Long> targetTable =
                builder.table(interimTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

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
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

}
