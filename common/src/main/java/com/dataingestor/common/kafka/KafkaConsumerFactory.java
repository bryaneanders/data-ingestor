package com.dataingestor.common.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Factory for creating Kafka consumers with defaults.
 * Consumes String keys and JSON-serialized values.
 */
public class KafkaConsumerFactory {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerFactory.class);

    /**
     * Create a Kafka consumer with default configuration.
     * Uses String deserializer for keys and values.
     *
     * @param config Kafka configuration
     * @param groupId consumer group ID
     * @return configured Kafka consumer
     */
    public static Consumer<String, String> createStringConsumer(KafkaConfig config, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        logger.info("Creating Kafka consumer with bootstrap servers: {}, groupId: {}",
                    config.getBootstrapServers(), groupId);
        return new KafkaConsumer<>(props);
    }
}
