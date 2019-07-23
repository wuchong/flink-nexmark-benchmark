package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery0;
import org.junit.Test;


public class Query0Test extends AbstractQueryTest{


    @Test
    public void run() throws Exception{
        String query = SqlQuery0.getQuery(flinkQueryRunner.getBidTable().toString());
        flinkQueryRunner.executeSqlQuery(query);
    }
}
