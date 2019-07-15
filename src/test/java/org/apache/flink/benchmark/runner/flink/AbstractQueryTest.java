/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark.runner.flink;

import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;
import org.apache.flink.benchmark.nexmark.model.*;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.flink.benchmark.nexmark.sources.generator.GeneratorConfig;
import org.apache.flink.benchmark.testutils.FileUtil;
import org.apache.flink.benchmark.testutils.TestUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.junit.Before;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractQueryTest {

    protected List<Event> inMemoryEvents = new ArrayList<>();

    protected String testPath = "./query_test_results";

    protected String sinkTableName = "CsvSinkTable";

    protected TableSink sink = new CsvTableSink(testPath, "|");

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractQueryTest.class);

    protected final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    protected final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    protected DataStream<Bid> bidStream;

    protected Table bidTable;

    protected DataStream<Auction> auctionStream;

    protected Table auctionTable;

    protected DataStream<Person> personStream;

    protected Table personTable;

    protected long numEvents = 10000L;

    protected GeneratorConfig initialConfig = makeConfig(numEvents);

    protected List<Category> inMemoryCategories;


    protected DataStream<Category> categoryStream;

    protected Table categoryTable;

    @Before
    public void before() {
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
                .map(event -> event.bid);
        bidTable = tableEnv.fromDataStream(bidStream,
                TestUtil.formatFields(Bid.getFieldNames()));
        auctionStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newAuction != null)
                .map(event -> event.newAuction);
        auctionTable = tableEnv.fromDataStream(auctionStream,
                TestUtil.formatFields(Auction.getFieldNames()));
        personStream = env.fromCollection(inMemoryEvents)
                .filter(event -> event.newPerson != null)
                .map(event -> event.newPerson);
        personTable = tableEnv.fromDataStream(personStream,
                TestUtil.formatFields(Person.getFieldNames()));

    }

    public abstract void run() throws Exception;

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

    public List<Category> prepareInMemoryCategories() {
        List<Category> categories = new ArrayList<>();
        for (int i = 0; i < GeneratorConfig.NUM_CATEGORIES; i++)
            categories.add(new Category(initialConfig.FIRST_CATEGORY_ID + i));
        return categories;
    }

    private GeneratorConfig makeConfig(long n) {
        return new GeneratorConfig(NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
    }
}
