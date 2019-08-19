package org.apache.flink.benchmark.runner.flink;

import org.junit.Before;

public class AbstractQueryTest {

    protected FlinkQueryRunner flinkQueryRunner;

    @Before
    public void before() throws Exception{
        flinkQueryRunner = new FlinkQueryRunner();
        flinkQueryRunner.init();
    }
}
