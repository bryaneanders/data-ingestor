package com.dataingestor.common.kafka;

/**
 * Configuration holder for Kafka connection settings.
 * Reads from environment variables by default.
 */
public class KafkaConfig {
    private final String bootstrapServers;

    public KafkaConfig() {
        this(getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
    }

    public KafkaConfig(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
