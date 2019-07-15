package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.testutils.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
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

    @Override
    @Test
    public void run() throws Exception {

        tableEnv.registerFunction(DolToEur.functionName, new DolToEur());
        tableEnv.registerTableSink(sinkTableName, Bid.getFieldNames(), Bid.getFieldTypes(), sink);


        String query1 = String.format("SELECT auction, bidder, %s(price) as price, extra FROM %s", DolToEur.functionName, bidTable) ;

        Table result = tableEnv.sqlQuery(query1);

        // Write Results to File

        result.insertInto(sinkTableName);

        env.execute();
    }




}
