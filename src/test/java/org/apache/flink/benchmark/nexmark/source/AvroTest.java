package org.apache.flink.benchmark.nexmark.source;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.avro.AvroEvent;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class AvroTest {

    Generator generator;

    int n = 10000;

    List<Event> events;

    String fileName = "event_bytes";

    @Before
    public void before() {
        generator = new Generator(Generator.makeConfig(n));
        events = generator.prepareInMemoryEvents(n);
    }

    @Test
    public void testEvent() throws Exception {


        DatumWriter<AvroEvent> eventDatumWriter = new SpecificDatumWriter<AvroEvent>(AvroEvent.class);
        DataFileWriter<AvroEvent> dataFileWriter = new DataFileWriter<AvroEvent>(eventDatumWriter);
        dataFileWriter.create(AvroEvent.SCHEMA$, new File(fileName));
        for (int i = 0; i < n; i++) {
            dataFileWriter.append(events.get(i).toAvro());
        }
        dataFileWriter.close();


        // Deserialize Users from disk
        DatumReader<AvroEvent> userDatumReader = new SpecificDatumReader<AvroEvent>(AvroEvent.class);
        DataFileReader<AvroEvent> dataFileReader = new DataFileReader<AvroEvent>(new File(fileName), userDatumReader);
        for(int i = 0; i < n; i++){
            AvroEvent avroEvent = dataFileReader.next();
            //System.out.println(avroEvent.toString());
            assertEquals(avroEvent.toString(), events.get(i).toAvro().toString());
        }

    }

    @Test
    public void testEncodeDecode() throws Exception{
        for(int i = 0; i < n; i++){
            AvroEvent encodeEvent = events.get(i).toAvro();
            ByteBuffer buf = AvroEvent.getEncoder().encode(encodeEvent);
            AvroEvent decodeEvent = AvroEvent.getDecoder().decode(buf);
            assertEquals(encodeEvent.toString(), decodeEvent.toString());
        }
    }

    @After
    public void after() {
        new File(fileName).deleteOnExit();
    }


}
