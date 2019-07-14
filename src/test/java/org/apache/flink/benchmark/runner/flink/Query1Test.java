package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.flink.benchmark.nexmark.sources.generator.GeneratorConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros. In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 * <p>To make things more interesting, allow the 'currency conversion' to be arbitrarily slowed
 * down.
 */

//TODO: print table snapshot in the console
//TODO: table sink creates many small files
//TODO: how to support Instant time?
public class Query1Test {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Query1Test.class);

    private List<Event> inMemoryEvents = new ArrayList<>();

    private String testPath = "./query0";

    @Before
    public void before() {
        long n = 15L;
        GeneratorConfig initialConfig = makeConfig(n);
        Generator generator = new Generator(initialConfig);
        for (int i = 0; i < n; i++) {
            Event event = generator.nextEvent().event;
            LOG.info(event.toString());
            inMemoryEvents.add(event);
        }
        File folder = new File(testPath);
        if(folder.exists())
            deleteFolder(folder);
    }

    public static class DolToEur extends ScalarFunction {
        public long eval(long price) {
            return price * 89 / 100;
        }
    }

    @Test
    public void run() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Bid> bidStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.bid != null)
                .map(event -> event.bid);

        TableSink sink = new CsvTableSink(testPath, "|");

        String[] fieldNames = {"auction", "bidder", "price", "extra"};

        TypeInformation[] fieldTypes = {Types.LONG(), Types.LONG(), Types.LONG(), Types.STRING()};

        tableEnv.registerFunction("DolToEur", new DolToEur());
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);

        Table bidTable = tableEnv.fromDataStream(bidStream,
                "auction, bidder, price, extra");


        String query1 = "SELECT auction, bidder, DolToEur(price) as price, extra FROM " + bidTable;

        Table result = tableEnv.sqlQuery(query1);

        // Write Results to File

        result.insertInto("CsvSinkTable");

        env.execute("query1");
    }

    private GeneratorConfig makeConfig(long n) {
        return new GeneratorConfig(NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }


}
