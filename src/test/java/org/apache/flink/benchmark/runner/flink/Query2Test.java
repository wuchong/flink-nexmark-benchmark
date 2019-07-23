package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery2;
import org.junit.Test;


public class Query2Test extends AbstractQueryTest {


    @Test
    public void run() throws Exception {

        String query2 = SqlQuery2.getQuery(flinkQueryRunner.getBidTable().toString());

        flinkQueryRunner.executeSqlQuery(query2);

    }
}
