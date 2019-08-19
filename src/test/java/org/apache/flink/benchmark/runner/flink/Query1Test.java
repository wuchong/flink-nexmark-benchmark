package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery1;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.Test;


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

//TODO: print table snapshot in the console
//TODO: table sink creates many small files
//TODO: how to support Instant time?
public class Query1Test extends AbstractQueryTest {


    public static class DolToEur extends ScalarFunction {

        public static final String functionName = "DolToEur";

        public long eval(long price) {
            return price * 89 / 100;
        }
    }


    @Test
    public void run() throws Exception {
        flinkQueryRunner.getTableEnv().registerFunction(DolToEur.functionName, new DolToEur());

        flinkQueryRunner.executeSqlQuery(SqlQuery1.getQuery(flinkQueryRunner.getBidTable().toString()),
                "query1_result");
    }




}
