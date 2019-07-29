package org.apache.flink.benchmark.nexmark.kafka;

import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.avro.AvroEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class EventSerializer implements Serializer<Event> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Event event) {
        try {
            return AvroEvent.getEncoder().encode(event.toAvro()).array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
