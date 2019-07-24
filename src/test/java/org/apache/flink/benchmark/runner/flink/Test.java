package org.apache.flink.benchmark.runner.flink;

public class Test extends AbstractQueryTest{

    @org.junit.Test
    public void run() throws Exception {
        String query = "select B.id, Max(B.pricde) from %1$s B GROUP BY B.id";

        flinkQueryRunner.executeSqlQuery(String.format(query, flinkQueryRunner.getBidTable().toString()));
    }
}
