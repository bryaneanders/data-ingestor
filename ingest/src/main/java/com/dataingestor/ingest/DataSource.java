package com.dataingestor.ingest;

import java.util.stream.Stream;

/**
 * Interface for reading data from various sources.
 * Implementations can read from files, feeds, Kafka topics, etc.
 */
public interface DataSource<T> {
    /**
     * Read data from the source as a stream of records.
     *
     * @return stream of records from the data source
     */
    Stream<T> ingest();
}
