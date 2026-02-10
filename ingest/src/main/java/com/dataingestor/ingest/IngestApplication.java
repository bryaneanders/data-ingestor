package com.dataingestor.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(
    name = "ingest",
    description = "Data ingestion application that reads from various sources and produces to Kafka",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class IngestApplication implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(IngestApplication.class);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new IngestApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting Ingest Application...");

        // TODO: Initialize Kafka producer and data sources

        logger.info("Ingest Application started successfully");
        return 0;
    }
}
