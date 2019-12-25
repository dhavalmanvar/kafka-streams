package com.github.dhavalmanvar.kafka.streams.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceStreamingApp {

    public static final String sourceTopic = "bank-transactions";
    public static final String targetTopic = "bank-balance";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonNodeSerde =
                Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions = builder.stream(sourceTopic,
                Consumed.with(Serdes.String(), jsonNodeSerde));

        KTable<String, JsonNode> targetTable = bankTransactions.groupByKey()
                .aggregate(
                        () -> getBalance(0, 0, Instant.ofEpochMilli(0L).toString()),
                        (user, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.with(Serdes.String(), jsonNodeSerde)
                );
        targetTable.toStream().to(targetTopic, Produced.with(Serdes.String(), jsonNodeSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();


    }

    public static ObjectNode getBalance(int count, int total, String time) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("count", count);
        node.put("total", total);
        node.put("time", time);
        return node;
    }

    public static ObjectNode newBalance(JsonNode transaction, JsonNode balance) {
        int count = balance.get("count").asInt() + 1;
        int total = balance.get("total").asInt() + transaction.get("amount").asInt();
        String time = transaction.get("time").asText();
        return getBalance(count, total, time);
    }
}
