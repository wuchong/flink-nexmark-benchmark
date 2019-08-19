package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery4;
import org.junit.Test;



public class Query4Test extends AbstractQueryTest{


    @Test
    public void run() throws Exception {
        String sql = SqlQuery4.getQuery(flinkQueryRunner.getCategoryTable().toString(),
                flinkQueryRunner.getAuctionTable().toString(),
                flinkQueryRunner.getBidTable().toString());
        flinkQueryRunner.executeSqlQuery(sql, "query4_result");
    }



}
