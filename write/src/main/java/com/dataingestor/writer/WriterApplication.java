package com.dataingestor.writer;

import com.dataingestor.common.kafka.KafkaConfig;
import com.dataingestor.common.kafka.KafkaConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(
    name = "write",
    description = "Data writer application that consumes from Kafka and writes to various data stores",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class WriterApplication implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(WriterApplication.class);

    @Option(names = {"-t", "--topic"}, description = "Kafka topic to consume from", required = true)
    private String topic;

    @Option(names = {"-o", "--output"}, description = "Output JSON file path", required = true)
    private String outputFile;

    @Option(names = {"-g", "--group"}, description = "Kafka consumer group ID", defaultValue = "writer-group")
    private String groupId;

    @Option(names = {"-m", "--max-records"}, description = "Maximum number of records to process (0 = infinite)", defaultValue = "0")
    private int maxRecords;

    @Option(names = {"--idle-timeout"}, description = "Stop after X seconds of no new messages (0 = disabled)", defaultValue = "0")
    private int idleTimeoutSeconds;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new WriterApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting Writer Application...");
        logger.info("Topic: {}, Output: {}, Group: {}", topic, outputFile, groupId);

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));

        KafkaConfig kafkaConfig = new KafkaConfig();

        try (Consumer<String, String> consumer = KafkaConsumerFactory.createStringConsumer(kafkaConfig, groupId);
             JsonFileSink sink = new JsonFileSink(outputFile)) {

            consumer.subscribe(Collections.singletonList(topic));
            logger.info("Subscribed to topic: {}", topic);

            int recordsProcessed = 0;
            long lastMessageTime = System.currentTimeMillis();

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.count() > 0) {
                    lastMessageTime = System.currentTimeMillis();
                }

                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("Consumed record from topic {} partition {} offset {}: key={}",
                        record.topic(), record.partition(), record.offset(), record.key());

                    sink.write(record.value());
                    recordsProcessed++;

                    // Check if we've hit max records
                    if (maxRecords > 0 && recordsProcessed >= maxRecords) {
                        logger.info("Reached max records limit: {}", maxRecords);
                        running.set(false);
                        break;
                    }
                }

                if (records.count() > 0) {
                    logger.info("Processed {} records (total: {})", records.count(), recordsProcessed);
                }

                // Check idle timeout
                if (idleTimeoutSeconds > 0) {
                    long idleTime = (System.currentTimeMillis() - lastMessageTime) / 1000;
                    if (idleTime >= idleTimeoutSeconds) {
                        logger.info("No messages received for {} seconds. Stopping.", idleTimeoutSeconds);
                        running.set(false);
                    }
                }
            }

            logger.info("Writer Application completed successfully. Total records: {}", recordsProcessed);
            return 0;

        } catch (Exception e) {
            logger.error("Writer Application failed", e);
            return 1;
        }
    }
}
