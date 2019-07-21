package org.apache.flink.benchmark.nexmark.queries;


/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros. In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 * <p>To make things more interesting, allow the 'currency conversion' to be arbitrarily slowed
 * down.
 */

public class SqlQuery1 {
    private static final String TEMPLATE = "SELECT auction, bidder, DolToEur(price) as price, ts, extra FROM %s";

    public static String getQuery(String bidTableName){
        return String.format(TEMPLATE, bidTableName);
    }
}
