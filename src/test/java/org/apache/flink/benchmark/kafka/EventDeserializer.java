package org.apache.flink.benchmark.kafka;

import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.avro.AvroEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class EventDeserializer implements Deserializer<Event> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Event deserialize(String topic, byte[] bytes) {
        try{
            return new Event(AvroEvent.getDecoder().decode(bytes));
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
