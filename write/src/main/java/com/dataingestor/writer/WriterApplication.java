package com.dataingestor.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(
    name = "write",
    description = "Data writer application that consumes from Kafka and writes to various data stores",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class WriterApplication implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(WriterApplication.class);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new WriterApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting Writer Application...");

        // TODO: Initialize Kafka consumer and data sinks

        logger.info("Writer Application started successfully");
        return 0;
    }
}
