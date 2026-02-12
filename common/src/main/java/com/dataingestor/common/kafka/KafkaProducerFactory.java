package com.dataingestor.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Factory for creating Kafka producers with defaults.
 * Produces String keys and JSON-serialized values.
 */
public class KafkaProducerFactory {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerFactory.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Create a Kafka producer with default configuration.
     * Uses String serializer for keys and values.
     *
     * @param config Kafka configuration
     * @return configured Kafka producer
     */
    public static Producer<String, String> createStringProducer(KafkaConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        logger.info("Creating Kafka producer with bootstrap servers: {}", config.getBootstrapServers());
        return new KafkaProducer<>(props);
    }

    /**
     * Get the shared ObjectMapper instance for JSON serialization.
     *
     * @return ObjectMapper instance
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
