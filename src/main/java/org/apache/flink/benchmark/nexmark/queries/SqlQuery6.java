package org.apache.flink.benchmark.nexmark.queries;


/**
 * Query 6, 'Average Selling Price by Seller'. Select the average selling price over the last 10
 * closed auctions by the same seller. In CQL syntax:
 *
 *
 * <pre>{@code
 * SELECT Istream(AVG(Q.final), Q.seller)
 * FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
 *       FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *       WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 *       GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
 * GROUP BY Q.seller;
 * }</pre>
 *
 * <p>We are a little more exact with selecting winning bids: see {@link WinningBids}.
 */


/**
 *  Query 6 test the semantic of 'Top-N' query
 *  Notice that some streaming system may implement 'Top-N' semantic in another way
 *  instead of standard SQL
 */
public class SqlQuery6 {

    private static final String TEMPLATE = "SELECT AVG(Q.final), Q.seller " +
            "  FROM (SELECT MAX(B.price) AS final, A.seller" +
            "        FROM %1$s A , %2$s B " +
            "        WHERE A.id=B.auction AND B.eventTime < A.expires AND A.expires < CURRENT_TIMESTAMP " +
            "        ORDER BY A.expires DESC LIMIT 10" +
            "        GROUP BY A.id, A.seller  )  Q " +
            "  GROUP BY Q.seller ";



    public static String getQuery(String auctionTableName, String bidTableName){
        return String.format(TEMPLATE, auctionTableName, bidTableName);
    }

}
