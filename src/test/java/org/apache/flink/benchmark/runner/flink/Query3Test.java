package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery3;
import org.junit.Test;


/**
 * Query 3, 'Local Item Suggestion'. Who is selling in OR, ID or CA in category 10, and for what
 * auction ids? In CQL syntax:
 *
 * <pre>
 * SELECT Istream(P.name, P.city, P.state, A.id)
 * FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
 * WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category
 * = 10;
 * </pre>
 *
 * <p>This implementation runs as written, but results may not be what is expected from a correct
 * join, and behavior doesn't match the java version.
 *
 * <p>At the moment join is implemented as a CoGBK, it joins the trigger outputs. It means that in
 * discarding mode it will join only new elements arrived since last trigger firing. And in
 * accumulating mode it will output the results which were already emitted in last trigger firing.
 *
 * <p>Additionally, it is currently not possible to match the elements across windows.
 *
 * <p>All of the above makes it not intuitive, inflexible, and produces results which may not be
 * what users are expecting.
 *
 * <p>Java version of the query ({@link Query3}) solves this by caching the auctions in the state
 * cell if there was no matching seller yet. And then flushes them when sellers become available.
 *
 * <p>Correct join semantics implementation is tracked in BEAM-3190, BEAM-3191
 */



public class Query3Test extends AbstractQueryTest {

    @Test
    public void run() throws Exception {
        String query = SqlQuery3.getQuery(flinkQueryRunner.getAuctionTable().toString(),
                flinkQueryRunner.getPersonTable().toString());
        flinkQueryRunner.executeSqlQuery(query, "query3_result");
    }
}
