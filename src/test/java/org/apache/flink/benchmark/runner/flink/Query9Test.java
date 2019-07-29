package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery9;
import org.junit.Ignore;
import org.junit.Test;


//TODO: Error : Auction' not found in table 'B'
@Ignore
public class Query9Test extends AbstractQueryTest{
    @Test
    public void run() throws Exception {
        String query = SqlQuery9.getQuery(flinkQueryRunner.getPersonTable().toString(),
                flinkQueryRunner.getAuctionTable().toString());
        flinkQueryRunner.executeSqlQuery(query);
    }
}
