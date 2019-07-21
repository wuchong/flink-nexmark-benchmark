package org.apache.flink.benchmark.nexmark.queries;


import org.apache.flink.benchmark.nexmark.model.Bid;

/**
 * Query 0: Pass events through unchanged.
 *
 * <p>This measures the overhead of the Flink SQL implementation and test harness like conversion
 * from Java model classes to Flink records.
 *
 * <p>{@link Bid} events are used here at the moment, ås they are most numerous with default
 * configuration.
 */
public class SqlQuery0 {

    private static final String TEMPLATE = "SELECT * FROM %s";

    public static String getQuery(String bidTableName){
        return String.format(TEMPLATE, bidTableName);
    }
}
