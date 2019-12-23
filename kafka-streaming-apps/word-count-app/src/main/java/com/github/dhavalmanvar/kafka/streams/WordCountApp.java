package com.github.dhavalmanvar.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountApp {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WordCountApp.class.getName());

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, String> source = streamsBuilder.stream("word-count-input");

        final KTable<String, Long> target = source
                .flatMapValues(value ->
                {
                    logger.info(value);
                    return Arrays.asList(value.toLowerCase().split(" "));
                })
                .groupBy((key, value) -> {
                    logger.info(value);
                    return value;
                })
                .count();

        target.toStream().to("word-count-output", Produced.with(stringSerde, longSerde));

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {

            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            logger.info(streams.toString());
            streams.start();
            latch.await();
        } catch (Exception ex) {
            logger.error("Caught exception: ", ex);
            System.exit(1);
        }
        System.exit(0);
    }
}
