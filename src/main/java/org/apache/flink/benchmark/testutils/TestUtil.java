package org.apache.flink.benchmark.testutils;

import org.apache.flink.benchmark.nexmark.NexmarkConfiguration;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sources.generator.Generator;
import org.apache.flink.benchmark.nexmark.sources.generator.GeneratorConfig;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TestUtil {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestUtil.class);

    public static String formatFields(String[] fields) {
        assert fields != null && fields.length != 0;
        if (fields.length == 1)
            return fields[0];
        StringBuilder sb = new StringBuilder();
        sb.append(fields[0]);
        for (int i = 1; i < fields.length; i++)
            sb.append(", " + fields[i]);
        return sb.toString();
    }


}
