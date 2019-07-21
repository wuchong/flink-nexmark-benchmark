package org.apache.flink.benchmark.nexmark.queries;


/**
 * Query 4, 'Average Price for a Category'. Select the average of the wining bid prices for all
 * closed auctions in each category. In CQL syntax:
 *
 * <pre>{@code
 * SELECT Istream(AVG(Q.final))
 * FROM Category C, (SELECT Rstream(MAX(B.price) AS final, A.category)
 *                   FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *                   WHERE A.id=B.auction AND B.timestamp < A.expires AND A.expires < CURRENT_TIME
 *                   GROUP BY A.id, A.category) Q
 * WHERE Q.category = C.id
 * GROUP BY C.id;
 * }</pre>
 *
 * <p>For extra spiciness our implementation differs slightly from the above:
 *
 * <ul>
 *   <li>We select both the average winning price and the category.
 *   <li>We only consider bids which are above the auction's reserve price.
 *   <li>We accept the highest-price, earliest valid bid as the winner.
 * </ul>
 */
public class SqlQuery4 {
    private static final String TEMPLATE = "" +
            "SELECT AVG(Q.final), C.id " +
            "  FROM %1$s C, (SELECT MAX(B.price) AS final, A.category " +
            "                    FROM %2$s A , %3$s B " +
            "                    WHERE A.id=B.auction AND B.ts < A.expires AND A.expires < CURRENT_TIMESTAMP " +
            "                    GROUP BY A.id, A.category) Q " +
            "  WHERE Q.category = C.id " +
            "  GROUP BY C.id";


    public static String getQuery(String categoryTableName, String auctionTableName, String bidTableName){
        return String.format(TEMPLATE, categoryTableName, auctionTableName, bidTableName);
    }
}
