package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;
import org.apache.flink.benchmark.nexmark.model.*;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.flink.benchmark.nexmark.sources.generator.GeneratorConfig;
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

import java.io.File;
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

    private long numEvents = 10000L;

    private GeneratorConfig initialConfig = makeConfig(numEvents);

    private List<Category> inMemoryCategories;

    private DataStream<Category> categoryStream;

    private Table categoryTable;

    public void init() {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        inMemoryEvents = prepareInMemoryEvents(numEvents);
        inMemoryCategories = prepareInMemoryCategories();

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
                        return bid.ts;
                    }
                });
        bidTable = tableEnv.fromDataStream(bidStream, "auction, bidder, price, ts.rowtime, extra");
        auctionStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newAuction != null)
                .map(event -> event.newAuction)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Auction>(
                        Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Auction auction) {
                        return auction.expires;
                    }
                });
        auctionTable = tableEnv.fromDataStream(auctionStream,
                "id, itemName, description, initialBid, reserve, ts, expires.rowtime, seller, " +
                        "category, extra");
        personStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newPerson != null)
                .map(event -> event.newPerson)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Person>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Person person) {
                        return person.ts;
                    }
                });
        personTable = tableEnv.fromDataStream(personStream, "id, name, emailAddress, " +
                "creditCard, city, state, ts.rowtime, extra");

    }

    public void executeSqlQuery(String sqlQuery) throws Exception{
        System.out.println(sqlQuery);
        Table result = tableEnv.sqlQuery(sqlQuery);
        DataStream<Tuple2<Boolean,Row>> stream = tableEnv.toRetractStream(result, Row.class);
        stream.print();
        env.execute();
    }


    private List<Event> prepareInMemoryEvents(long n) {

        Generator generator = new Generator(initialConfig);
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Event event = generator.nextEvent().event;
            //LOG.info(event.toString());
            events.add(event);
        }
        return events;
    }

    private List<Category> prepareInMemoryCategories() {
        List<Category> categories = new ArrayList<>();
        for (int i = 0; i < GeneratorConfig.NUM_CATEGORIES; i++)
            categories.add(new Category(initialConfig.FIRST_CATEGORY_ID + i));
        return categories;
    }

    private GeneratorConfig makeConfig(long n) {
        return new GeneratorConfig(NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
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
