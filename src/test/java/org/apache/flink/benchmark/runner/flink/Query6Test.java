package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.queries.SqlQuery6;
import org.junit.Ignore;
import org.junit.Test;


//TODO: Test Fail. SQL Syntax Error
//TODO: how to implement [PARTITION BY A.seller ROWS 10]? Based on event time or processing time?
//TODO: check whether 'ORDER BY' is the right implementation
@Ignore
public class Query6Test extends AbstractQueryTest{

    @Test
    public void run() throws Exception {

        String query = SqlQuery6.getQuery(flinkQueryRunner.getAuctionTable().toString(),
                flinkQueryRunner.getBidTable().toString());

        flinkQueryRunner.executeSqlQuery(query);
    }
}
