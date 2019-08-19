package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.nexmark.model.*;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.flink.benchmark.testutils.FileUtil;
import org.apache.flink.benchmark.testutils.TestUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class FlinkQueryRunner {
    private List<Event> inMemoryEvents = new ArrayList<>();

    private String testPath = "./query_test_results";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FlinkQueryRunner.class);

    private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    private DataStream<Bid> bidStream;

    private Table bidTable;

    private DataStream<Auction> auctionStream;

    private Table auctionTable;

    private DataStream<Person> personStream;

    private Table personTable;

    private long numEvents = 100000L;

    //private GeneratorConfig initialConfig = makeConfig(numEvents);

    private List<Category> inMemoryCategories;

    private DataStream<Category> categoryStream;

    private Table categoryTable;

    private Generator generator;

    public void init() throws Exception {
        generator = new Generator(Generator.makeConfig(numEvents));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        inMemoryEvents = generator.prepareInMemoryEvents(numEvents);
        inMemoryCategories = generator.prepareInMemoryCategories();

        File folder = new File(testPath);
        if (folder.exists())
            FileUtil.deleteFolder(folder);
        categoryStream = env.fromCollection(inMemoryCategories);
        categoryTable = tableEnv.fromDataStream(categoryStream,
                TestUtil.formatFields(Category.getFieldNames()));
        bidStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.bid != null)
                .map(event -> event.bid)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Bid>(
                        Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Bid bid) {
                        return bid.ts.getTime();
                    }
                });
        bidTable = tableEnv.fromDataStream(bidStream, "auction, bidder, price, " +
                "ts, extra, eventTime.rowtime");
        auctionStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newAuction != null)
                .map(event -> event.newAuction)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Auction>(
                        Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Auction auction) {
                        return auction.expires.getTime();
                    }
                });
        auctionTable = tableEnv.fromDataStream(auctionStream,
                "id, itemName, description, initialBid, reserve, ts, expires, seller, " +
                        "category, extra, eventTime.rowtime");
        personStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newPerson != null)
                .map(event -> event.newPerson)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Person>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Person person) {
                        return person.ts.getTime();
                    }
                });
        personTable = tableEnv.fromDataStream(personStream, "id, name, emailAddress, " +
                "creditCard, city, state, ts, extra, eventTime.rowtime");

    }

    public void executeSqlQuery(String sqlQuery, String outputFileName) throws Exception {
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFileName, false), 4096));
        System.setOut(ps);
        LOG.info(sqlQuery);
        Table result = tableEnv.sqlQuery(sqlQuery);
        DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(result, Row.class);
        stream.print();
        env.execute();
    }


    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }

    public Table getBidTable() {
        return bidTable;
    }

    public Table getAuctionTable() {
        return auctionTable;
    }

    public Table getPersonTable() {
        return personTable;
    }

    public Table getCategoryTable() {
        return categoryTable;
    }


}
