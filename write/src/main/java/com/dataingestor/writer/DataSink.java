package com.dataingestor.writer;

/**
 * Interface for writing refined data to various destinations.
 * Implementations can write to SQL databases, JSON files, etc.
 */
public interface DataSink<T> {
    /**
     * Write a refined record to the sink.
     *
     * @param record the record to write
     */
    void write(T record);
}
