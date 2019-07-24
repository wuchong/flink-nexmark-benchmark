package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery8;
import org.junit.Test;


public class Query8Test extends AbstractQueryTest{

    @Test
    public void run() throws Exception {
        String query = SqlQuery8.getQuery(flinkQueryRunner.getPersonTable().toString(),
                flinkQueryRunner.getAuctionTable().toString());
        flinkQueryRunner.executeSqlQuery(query);
    }
}
