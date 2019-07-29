package org.apache.flink.benchmark.kafka;


import org.apache.flink.benchmark.nexmark.kafka.EventConsumerCreator;
import org.apache.flink.benchmark.nexmark.kafka.EventProducerCreator;
import org.apache.flink.benchmark.nexmark.kafka.IKafkaConstants;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;


@Ignore
public class KafkaTest {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaTest.class);

    Generator generator;

    int n = 10000;

    List<Event> events;


    @Before
    public void before() {
        generator = new Generator(Generator.makeConfig(n));
        events = generator.prepareInMemoryEvents(n);
    }

    @Test
    public void avroEventTest() throws Exception{
        //produce message to topic
        Producer<Integer, Event> producer = EventProducerCreator.createProducer();
        List<Future<RecordMetadata>> completeList = new ArrayList<>();
        for(int i = 0; i < n; i++){
            Event event = events.get(i);
            Future<RecordMetadata> complete = producer.send(
                    new ProducerRecord<>(IKafkaConstants.TOPIC_NAME,event));
            completeList.add(complete);
        }
        for(int i = 0; i < n; i++){
            completeList.get(i).get();
        }

        LOGGER.info("produce message to broker");

        //consumer message from broker and verify the result
        Consumer<Integer, Event> consumer = EventConsumerCreator.createConsumer();
        int noMessageFound = 0;
        int cnt = 0;
        while (true) {
            LOGGER.info("cnt : " + cnt);
            ConsumerRecords<Integer, Event> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            Iterator<ConsumerRecord<Integer, Event>> records = consumerRecords.records(IKafkaConstants.TOPIC_NAME).iterator();
            while(records.hasNext()){
                Event event = records.next().value();
                assertEquals(event, events.get(cnt));
                cnt++;
            }

            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
        LOGGER.info("finish consuming message");
    }


}
