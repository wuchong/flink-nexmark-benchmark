package org.apache.flink.benchmark.nexmark.queries;


import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;

/**
 * Query 7, 'Highest Bid'. Select the bids with the highest bid price in the last minute. In CQL
 * syntax:
 *
 * <pre>
 * SELECT Rstream(B.auction, B.price, B.bidder)
 * FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
 * WHERE B.price = (SELECT MAX(B1.price)
 *                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
 * </pre>
 *
 * <p>We will use a shorter window to help make testing easier. We'll also implement this using a
 * side-input in order to exercise that functionality. (A combiner, as used in Query 5, is a more
 * efficient approach.).
 */

public class SqlQuery7 {
    private static final String TEMPLATE =
            ""
                    + " SELECT B.auction, B.price, B.bidder, B.extra "
                    + "    FROM (SELECT B.auction, B.price, B.bidder, B.ts, B.extra, "
                    + "       TUMBLE_START(B.ts, INTERVAL '%1$d' SECOND) AS starttime "
                    + "    FROM %2$s B "
                    + "    GROUP BY B.auction, B.price, B.bidder, B.ts, B.extra, "
                    + "       TUMBLE(B.ts, INTERVAL '%1$d' SECOND)) B "
                    + " JOIN (SELECT MAX(B1.price) AS maxprice, "
                    + "       TUMBLE_START(B1.ts, INTERVAL '%1$d' SECOND) AS starttime "
                    + "    FROM %2$s B1 "
                    + "    GROUP BY TUMBLE(B1.ts, INTERVAL '%1$d' SECOND)) B1 "
                    + " ON B.starttime = B1.starttime AND B.price = B1.maxprice ";

    public static String getQuery(String bidTableName){
        return String.format(TEMPLATE, NexmarkConfiguration.DEFAULT.windowSizeSec, bidTableName);
    }
}
