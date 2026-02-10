package com.dataingestor.processing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(
    name = "process",
    description = "Data processing application that consumes from Kafka, transforms data, and produces back to Kafka",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class ProcessingApplication implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(ProcessingApplication.class);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ProcessingApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting Processing Application...");

        // TODO: Initialize Kafka consumer and producer for processing pipeline

        logger.info("Processing Application started successfully");
        return 0;
    }
}
