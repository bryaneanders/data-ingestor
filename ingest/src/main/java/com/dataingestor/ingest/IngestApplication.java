package com.dataingestor.ingest;

import com.dataingestor.common.kafka.KafkaConfig;
import com.dataingestor.common.kafka.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(
    name = "ingest",
    description = "Data ingestion application that reads from various sources and produces to Kafka",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class IngestApplication implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(IngestApplication.class);

    @Option(names = {"-f", "--file"}, description = "CSV file path to ingest", required = true)
    private String filePath;

    @Option(names = {"-t", "--topic"}, description = "Kafka topic to produce to", required = true)
    private String topic;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new IngestApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting Ingest Application...");
        logger.info("File: {}, Topic: {}", filePath, topic);

        KafkaConfig kafkaConfig = new KafkaConfig();

        try (Producer<String, String> producer = KafkaProducerFactory.createStringProducer(kafkaConfig)) {
            DataSource<String> dataSource = new BasicCSVDataSource(filePath, topic, producer);
            dataSource.ingest();

            logger.info("Ingest Application completed successfully");
            return 0;

        } catch (Exception e) {
            logger.error("Ingest Application failed", e);
            return 1;
        }
    }
}
