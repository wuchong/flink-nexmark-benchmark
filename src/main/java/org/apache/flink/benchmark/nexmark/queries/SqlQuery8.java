package org.apache.flink.benchmark.nexmark.queries;


import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;

/**
 * Query 8, 'Monitor New Users'. Select people who have entered the system and created auctions in
 * the last 12 hours, updated every 12 hours. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(P.id, P.name, A.reserve)
 * FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
 * WHERE P.id = A.seller;
 * </pre>
 *
 * <p>To make things a bit more dynamic and easier to test we'll use a much shorter window.
 */

public class SqlQuery8 {
    private static final String TEMPLATE = "" +
            " SELECT PersonWindow.id, PersonWindow.name, AuctionWindow.reserve " +
            " FROM (" +
            "  SELECT P.id, P.name, HOP_START(P.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime " +
            "  FROM %3$s P " +
            "  GROUP BY P.id, P.name, HOP(P.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)" +
            "  ) AS PersonWindow" +
            " JOIN (" +
            "   SELECT A.seller, A.reserve, HOP_START(A.expires, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime" +
            "   FROM %4$s A" +
            "   GROUP BY A.seller, A.reserve, HOP(A.expires, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)" +
            "  ) AS AuctionWindow" +
            " ON PersonWindow.starttime = AuctionWindow.starttime " +
            " AND PersonWindow.id = AuctionWindow.seller";


    public static String getQuery(String personTableName, String auctionTableName){
        return String.format(TEMPLATE, NexmarkConfiguration.DEFAULT.windowPeriodSec,
                NexmarkConfiguration.DEFAULT.windowSizeSec, personTableName, auctionTableName);
    }

}
