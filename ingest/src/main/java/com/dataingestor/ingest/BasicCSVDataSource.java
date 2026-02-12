package com.dataingestor.ingest;

import com.dataingestor.common.kafka.KafkaProducerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicCSVDataSource implements DataSource<String> {
    private static final Logger logger = LoggerFactory.getLogger(BasicCSVDataSource.class);
    private static final ObjectMapper objectMapper = KafkaProducerFactory.getObjectMapper();

    private final String filePath;
    private final String topic;
    private final Producer<String, String> producer;

    public BasicCSVDataSource(String filePath, String topic, Producer<String, String> producer) {
        this.filePath = filePath;
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void ingest() {
        logger.info("Starting CSV ingestion from file: {}", filePath);

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();

            if (records.isEmpty()) {
                logger.warn("CSV file is empty: {}", filePath);
                return;
            }

            // First row is header
            String[] headers = records.get(0);
            logger.info("CSV headers: {}", String.join(", ", headers));

            // Process remaining rows
            int recordCount = 0;
            for (int i = 1; i < records.size(); i++) {
                String[] row = records.get(i);

                // Convert row to JSON object
                Map<String, String> recordMap = new HashMap<>();
                for (int j = 0; j < headers.length && j < row.length; j++) {
                    recordMap.put(headers[j], row[j]);
                }

                // Serialize to JSON
                String jsonValue = objectMapper.writeValueAsString(recordMap);

                // Send to Kafka (using row number as key)
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, String.valueOf(i), jsonValue);

                producer.send(kafkaRecord, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to send record to Kafka", exception);
                    } else {
                        logger.debug("Sent record to topic {} partition {} offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });

                recordCount++;
            }

            producer.flush();
            logger.info("Successfully ingested {} records from CSV to topic {}", recordCount, topic);

        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", filePath, e);
            throw new RuntimeException("Failed to read CSV file", e);
        } catch (CsvException e) {
            logger.error("Error parsing CSV file: {}", filePath, e);
            throw new RuntimeException("Failed to parse CSV file", e);
        }
    }
}
