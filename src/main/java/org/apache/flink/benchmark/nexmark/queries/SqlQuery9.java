package org.apache.flink.benchmark.nexmark.queries;

/**
 * Query "9", 'Winning bids'. Select just the winning bids. Not in original NEXMark suite, but handy
 * for testing.
 */

/**
 * SELECT Rstream(A.*, B.auction, B.bidder, MAX(B.price), B.dateTime)
 *  * FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *  * WHERE A.id = B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 *  * GROUP BY A.id
 */
public class SqlQuery9 {
    private static final String TEMPLATE = "" +
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC) AS rownum " +
            "FROM %1$s A , %2$s B " +
            "WHERE A.id = B.auction AND B.datetime < A.expires " +
            "   AND A.expires < CURRENT_TIME AND rownum <= 1 ";


    public static String getQuery(String auctionTableName, String bidTableName){
        return String.format(TEMPLATE, auctionTableName, bidTableName);
    }
}
