package org.apache.flink.benchmark.runner.flink;


import org.apache.flink.benchmark.nexmark.queries.SqlQuery5;
import org.junit.Test;



public class Query5Test extends AbstractQueryTest{

    @Test
    public void run() throws Exception {
        String sql = SqlQuery5.getQuery(flinkQueryRunner.getBidTable().toString());
        flinkQueryRunner.executeSqlQuery(sql, "query5_result");
    }
}
