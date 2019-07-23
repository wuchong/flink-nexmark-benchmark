package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery7;
import org.junit.Test;



//TODO: Test Fail.
public class Query7Test extends AbstractQueryTest{

    @Test
    public void run() throws Exception {
        String query = SqlQuery7.getQuery(flinkQueryRunner.getBidTable().toString());
        flinkQueryRunner.executeSqlQuery(query);
    }
}
