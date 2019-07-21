package org.apache.flink.benchmark.nexmark.queries;


import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Joiner;
import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class SqlQuery5 {
    private static final String TEMPLATE =
            Joiner.on("\n\t")
                    .join(
                            " SELECT AuctionBids.auction, AuctionBids.num",
                            " FROM (",
                            "   SELECT",
                            "     B1.auction,",
                            "     count(*) AS num,",
                            "     HOP_START(B1.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime",
                            "   FROM %3$s B1 ",
                            "   GROUP BY ",
                            "     B1.auction,",
                            "     HOP(B1.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)",
                            " ) AS AuctionBids",
                            " JOIN (",
                            "   SELECT ",
                            "     max(CountBids.num) AS maxnum, ",
                            "     CountBids.starttime",
                            "   FROM (",
                            "     SELECT",
                            "       count(*) AS num,",
                            "       HOP_START(B2.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND) AS starttime",
                            "     FROM %3$s B2 ",
                            "     GROUP BY ",
                            "       B2.auction, ",
                            "       HOP(B2.ts, INTERVAL '%1$d' SECOND, INTERVAL '%2$d' SECOND)",
                            "     ) AS CountBids",
                            "   GROUP BY CountBids.starttime",
                            " ) AS MaxBids ",
                            " ON AuctionBids.starttime = MaxBids.starttime AND AuctionBids.num >= MaxBids.maxnum ");




    public static String getQuery(String bidTableName){
        return String.format(TEMPLATE,
                        NexmarkConfiguration.DEFAULT.windowPeriodSec,
                        NexmarkConfiguration.DEFAULT.windowSizeSec,
                        bidTableName);
    }
}
