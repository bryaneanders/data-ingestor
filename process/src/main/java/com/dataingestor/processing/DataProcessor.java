package com.dataingestor.processing;

/**
 * Interface for processing unrefined data into refined records.
 */
public interface DataProcessor<I, O> {
    /**
     * Process an unrefined record into a refined record.
     *
     * @param input the unrefined input record
     * @return the refined output record
     */
    O process(I input);
}
