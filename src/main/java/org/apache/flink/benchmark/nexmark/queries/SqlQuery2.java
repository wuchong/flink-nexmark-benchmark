package org.apache.flink.benchmark.nexmark.queries;


/**
 * Query 2, 'Filtering. Find bids with specific auction ids and show their bid price. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(auction, price)
 * FROM Bid [NOW]
 * WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
 * </pre>
 *
 * <p>As written that query will only yield a few hundred results over event streams of arbitrary
 * size. To make it more interesting we instead choose bids for every {@code skipFactor}'th auction.
 */
public class SqlQuery2 {
    private static final String TEMPLATE = "" +
            "SELECT auction, price " +
            "FROM %1$s " +
            "WHERE MOD(auction, %2$d) = 0";

    private static final int skip_factor = 2;

    public static String getQuery(String bidTableName){
        return String.format(TEMPLATE, bidTableName, skip_factor);
    }
}
