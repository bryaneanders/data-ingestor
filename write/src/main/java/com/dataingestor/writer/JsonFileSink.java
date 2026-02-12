package com.dataingestor.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * DataSink that writes records as JSON lines to a file.
 * Each record is written as a separate line (JSONL format).
 */
public class JsonFileSink implements DataSink<String>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JsonFileSink.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String filePath;
    private final BufferedWriter writer;
    private int recordCount = 0;

    public JsonFileSink(String filePath) throws IOException {
        this.filePath = filePath;

        // Ensure parent directory exists
        Files.createDirectories(Paths.get(filePath).getParent());

        this.writer = new BufferedWriter(new FileWriter(filePath, false));
        logger.info("Initialized JSON file sink: {}", filePath);
    }

    @Override
    public void write(String record) {
        try {
            // Validate it's valid JSON
            objectMapper.readTree(record);

            // Write as JSONL (one JSON object per line)
            writer.write(record);
            writer.newLine();
            recordCount++;

            logger.debug("Wrote record #{} to {}", recordCount, filePath);

        } catch (IOException e) {
            logger.error("Failed to write record to file: {}", filePath, e);
            throw new RuntimeException("Failed to write to JSON file", e);
        }
    }

    /**
     * Flush and close the writer.
     * Should be called when done writing.
     */
    public void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
            logger.info("Closed JSON file sink: {} ({} records written)", filePath, recordCount);
        }
    }

    public int getRecordCount() {
        return recordCount;
    }
}
