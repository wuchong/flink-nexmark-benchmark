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
package org.apache.flink.benchmark.nexmark;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.flink.benchmark.nexmark.model.*;
import org.apache.flink.benchmark.nexmark.queries.NexmarkQuery;
import org.apache.flink.benchmark.nexmark.queries.NexmarkQueryModel;
import org.apache.flink.benchmark.nexmark.queries.NexmarkQueryUtil;
import org.apache.flink.benchmark.testutils.metrics.MetricsReader;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

/**
 * Run a single Nexmark query using a given configuration.
 */
public class NexmarkLauncher<OptionT extends NexmarkOptions> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkLauncher.class);

    /**
     * Command line parameter value for query language.
     */
    private static final String SQL = "sql";

    /**
     * Minimum number of samples needed for 'stead-state' rate calculation.
     */
    private static final int MIN_SAMPLES = 9;
    /**
     * Minimum length of time over which to consider samples for 'steady-state' rate calculation.
     */
    private static final Duration MIN_WINDOW = Duration.standardMinutes(2);
    /**
     * Delay between perf samples.
     */
    private static final Duration PERF_DELAY = Duration.standardSeconds(15);
    /**
     * How long to let streaming pipeline run after all events have been generated and we've seen no
     * activity.
     */
    private static final Duration DONE_DELAY = Duration.standardMinutes(1);
    /**
     * How long to allow no activity at sources and sinks without warning.
     */
    private static final Duration STUCK_WARNING_DELAY = Duration.standardMinutes(10);
    /**
     * How long to let streaming pipeline run after we've seen no activity at sources or sinks, even
     * if all events have not been generated.
     */
    private static final Duration STUCK_TERMINATE_DELAY = Duration.standardHours(1);

    /**
     * NexmarkOptions for this run.
     */
    private final OptionT options;

    /**
     * Which configuration we are running.
     */
    private NexmarkConfiguration configuration;

    /**
     * If in --pubsubMode=COMBINED, the event monitor for the publisher pipeline. Otherwise null.
     */
    @Nullable
    private Monitor<Event> publisherMonitor;

    /**
     * If in --pubsubMode=COMBINED, the pipeline result for the publisher pipeline. Otherwise null.
     */
    @Nullable
    private PipelineResult publisherResult;

    /**
     * Result for the main pipeline.
     */
    @Nullable
    private PipelineResult mainResult;

    /**
     * Query name we are running.
     */
    @Nullable
    private String queryName;

    /**
     * Full path of the PubSub topic (when PubSub is enabled).
     */
    @Nullable
    private String pubsubTopic;

    /**
     * Full path of the PubSub subscription (when PubSub is enabled).
     */
    @Nullable
    private String pubsubSubscription;


    public NexmarkLauncher(OptionT options, NexmarkConfiguration configuration) {
        this.options = options;
        this.configuration = configuration;
    }

    /**
     * Is this query running in streaming mode?
     */
    private boolean isStreaming() {
        return options.isStreaming();
    }

    /**
     * Return maximum number of workers.
     */
    private int maxNumWorkers() {
        return 5;
    }

    /**
     * Find a 'steady state' events/sec from {@code snapshots} and store it in {@code perf} if found.
     */
    private void captureSteadyState(NexmarkPerf perf, List<NexmarkPerf.ProgressSnapshot> snapshots) {
        if (!options.isStreaming()) {
            return;
        }

        // Find the first sample with actual event and result counts.
        int dataStart = 0;
        for (; dataStart < snapshots.size(); dataStart++) {
            if (snapshots.get(dataStart).numEvents >= 0 && snapshots.get(dataStart).numResults >= 0) {
                break;
            }
        }

        // Find the last sample which demonstrated progress.
        int dataEnd = snapshots.size() - 1;
        for (; dataEnd > dataStart; dataEnd--) {
            if (snapshots.get(dataEnd).anyActivity(snapshots.get(dataEnd - 1))) {
                break;
            }
        }

        int numSamples = dataEnd - dataStart + 1;
        if (numSamples < MIN_SAMPLES) {
            // Not enough samples.
            NexmarkUtils.console(
                    "%d samples not enough to calculate steady-state event rate", numSamples);
            return;
        }

        // We'll look at only the middle third samples.
        int sampleStart = dataStart + numSamples / 3;
        int sampleEnd = dataEnd - numSamples / 3;

        double sampleSec =
                snapshots.get(sampleEnd).secSinceStart - snapshots.get(sampleStart).secSinceStart;
        if (sampleSec < MIN_WINDOW.getStandardSeconds()) {
            // Not sampled over enough time.
            NexmarkUtils.console(
                    "sample of %.1f sec not long enough to calculate steady-state event rate", sampleSec);
            return;
        }

        // Find rate with least squares error.
        double sumxx = 0.0;
        double sumxy = 0.0;
        long prevNumEvents = -1;
        for (int i = sampleStart; i <= sampleEnd; i++) {
            if (prevNumEvents == snapshots.get(i).numEvents) {
                // Skip samples with no change in number of events since they contribute no data.
                continue;
            }
            // Use the effective runtime instead of wallclock time so we can
            // insulate ourselves from delays and stutters in the query manager.
            double x = snapshots.get(i).runtimeSec;
            prevNumEvents = snapshots.get(i).numEvents;
            double y = prevNumEvents;
            sumxx += x * x;
            sumxy += x * y;
        }
        double eventsPerSec = sumxy / sumxx;
        NexmarkUtils.console("revising events/sec from %.1f to %.1f", perf.eventsPerSec, eventsPerSec);
        perf.eventsPerSec = eventsPerSec;
    }

    /**
     * Return the current performance given {@code eventMonitor} and {@code resultMonitor}.
     */
    private NexmarkPerf currentPerf(
            long startMsSinceEpoch,
            long now,
            PipelineResult result,
            List<NexmarkPerf.ProgressSnapshot> snapshots,
            Monitor<?> eventMonitor,
            Monitor<?> resultMonitor) {
        NexmarkPerf perf = new NexmarkPerf();

        MetricsReader eventMetrics = new MetricsReader(result, eventMonitor.name);

        long numEvents = eventMetrics.getCounterMetric(eventMonitor.prefix + ".elements");
        long numEventBytes = eventMetrics.getCounterMetric(eventMonitor.prefix + ".bytes");
        long eventStart = eventMetrics.getStartTimeMetric(eventMonitor.prefix + ".startTime");
        long eventEnd = eventMetrics.getEndTimeMetric(eventMonitor.prefix + ".endTime");

        MetricsReader resultMetrics = new MetricsReader(result, resultMonitor.name);

        long numResults = resultMetrics.getCounterMetric(resultMonitor.prefix + ".elements");
        long numResultBytes = resultMetrics.getCounterMetric(resultMonitor.prefix + ".bytes");
        long resultStart = resultMetrics.getStartTimeMetric(resultMonitor.prefix + ".startTime");
        long resultEnd = resultMetrics.getEndTimeMetric(resultMonitor.prefix + ".endTime");
        long timestampStart =
                resultMetrics.getStartTimeMetric(resultMonitor.prefix + ".startTimestamp");
        long timestampEnd = resultMetrics.getEndTimeMetric(resultMonitor.prefix + ".endTimestamp");

        long effectiveEnd = -1;
        if (eventEnd >= 0 && resultEnd >= 0) {
            // It is possible for events to be generated after the last result was emitted.
            // (Eg Query 2, which only yields results for a small prefix of the event stream.)
            // So use the max of last event and last result times.
            effectiveEnd = Math.max(eventEnd, resultEnd);
        } else if (resultEnd >= 0) {
            effectiveEnd = resultEnd;
        } else if (eventEnd >= 0) {
            // During startup we may have no result yet, but we would still like to track how
            // long the pipeline has been running.
            effectiveEnd = eventEnd;
        }

        if (effectiveEnd >= 0 && eventStart >= 0 && effectiveEnd >= eventStart) {
            perf.runtimeSec = (effectiveEnd - eventStart) / 1000.0;
        }

        if (numEvents >= 0) {
            perf.numEvents = numEvents;
        }

        if (numEvents >= 0 && perf.runtimeSec > 0.0) {
            // For streaming we may later replace this with a 'steady-state' value calculated
            // from the progress snapshots.
            perf.eventsPerSec = numEvents / perf.runtimeSec;
        }

        if (numEventBytes >= 0 && perf.runtimeSec > 0.0) {
            perf.eventBytesPerSec = numEventBytes / perf.runtimeSec;
        }

        if (numResults >= 0) {
            perf.numResults = numResults;
        }

        if (numResults >= 0 && perf.runtimeSec > 0.0) {
            perf.resultsPerSec = numResults / perf.runtimeSec;
        }

        if (numResultBytes >= 0 && perf.runtimeSec > 0.0) {
            perf.resultBytesPerSec = numResultBytes / perf.runtimeSec;
        }

        if (eventStart >= 0) {
            perf.startupDelaySec = (eventStart - startMsSinceEpoch) / 1000.0;
        }

        if (resultStart >= 0 && eventStart >= 0 && resultStart >= eventStart) {
            perf.processingDelaySec = (resultStart - eventStart) / 1000.0;
        }

        if (timestampStart >= 0 && timestampEnd >= 0 && perf.runtimeSec > 0.0) {
            double eventRuntimeSec = (timestampEnd - timestampStart) / 1000.0;
            perf.timeDilation = eventRuntimeSec / perf.runtimeSec;
        }

        if (resultEnd >= 0) {
            // Fill in the shutdown delay assuming the job has now finished.
            perf.shutdownDelaySec = (now - resultEnd) / 1000.0;
        }

        // As soon as available, try to capture cumulative cost at this point too.

        NexmarkPerf.ProgressSnapshot snapshot = new NexmarkPerf.ProgressSnapshot();
        snapshot.secSinceStart = (now - startMsSinceEpoch) / 1000.0;
        snapshot.runtimeSec = perf.runtimeSec;
        snapshot.numEvents = numEvents;
        snapshot.numResults = numResults;
        snapshots.add(snapshot);

        captureSteadyState(perf, snapshots);

        return perf;
    }

    /**
     * Build and run a pipeline using specified options.
     */
    interface PipelineBuilder<OptionT extends NexmarkOptions> {
        void build(OptionT publishOnlyOptions);
    }

    /**
     * Invoke the builder with options suitable for running a publish-only child pipeline.
     */
    private void invokeBuilderForPublishOnlyPipeline(PipelineBuilder<NexmarkOptions> builder) {
        String jobName = options.getJobName();
        String appName = options.getAppName();
        int numWorkers = options.getNumWorkers();
        int maxNumWorkers = options.getMaxNumWorkers();

        options.setJobName("p-" + jobName);
        options.setAppName("p-" + appName);
        int eventGeneratorWorkers = configuration.numEventGenerators;
        // TODO: assign one generator per core rather than one per worker.
        if (numWorkers > 0 && eventGeneratorWorkers > 0) {
            options.setNumWorkers(Math.min(numWorkers, eventGeneratorWorkers));
        }
        if (maxNumWorkers > 0 && eventGeneratorWorkers > 0) {
            options.setMaxNumWorkers(Math.min(maxNumWorkers, eventGeneratorWorkers));
        }
        try {
            builder.build(options);
        } finally {
            options.setJobName(jobName);
            options.setAppName(appName);
            options.setNumWorkers(numWorkers);
            options.setMaxNumWorkers(maxNumWorkers);
        }
    }

    /**
     * Monitor the performance and progress of a running job. Return final performance if it was
     * measured.
     */
    @Nullable
    private NexmarkPerf monitor(NexmarkQuery query) {
        if (!options.getMonitorJobs()) {
            return null;
        }

        if (configuration.debug) {
            NexmarkUtils.console("Waiting for main pipeline to 'finish'");
        } else {
            NexmarkUtils.console("--debug=false, so job will not self-cancel");
        }

        PipelineResult job = mainResult;
        PipelineResult publisherJob = publisherResult;
        List<NexmarkPerf.ProgressSnapshot> snapshots = new ArrayList<>();
        long startMsSinceEpoch = System.currentTimeMillis();
        long endMsSinceEpoch = -1;
        if (options.getRunningTimeMinutes() != null) {
            endMsSinceEpoch =
                    startMsSinceEpoch
                            + Duration.standardMinutes(options.getRunningTimeMinutes()).getMillis()
                            - Duration.standardSeconds(configuration.preloadSeconds).getMillis();
        }
        long lastActivityMsSinceEpoch = -1;
        NexmarkPerf perf = null;
        boolean waitingForShutdown = false;
        boolean cancelJob = false;
        boolean publisherCancelled = false;
        List<String> errors = new ArrayList<>();

        while (true) {
            long now = System.currentTimeMillis();
            if (endMsSinceEpoch >= 0 && now > endMsSinceEpoch && !waitingForShutdown) {
                NexmarkUtils.console("Reached end of test, cancelling job");
                try {
                    cancelJob = true;
                    job.cancel();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to cancel main job: ", e);
                }
                if (publisherResult != null) {
                    try {
                        publisherJob.cancel();
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to cancel publisher job: ", e);
                    }
                    publisherCancelled = true;
                }
                waitingForShutdown = true;
            }

            PipelineResult.State state = job.getState();
            NexmarkUtils.console(
                    "%s %s%s", state, queryName, waitingForShutdown ? " (waiting for shutdown)" : "");

            NexmarkPerf currPerf;
            if (configuration.debug) {
                currPerf =
                        currentPerf(
                                startMsSinceEpoch, now, job, snapshots, query.eventMonitor, query.resultMonitor);
            } else {
                currPerf = null;
            }

            if (perf == null || perf.anyActivity(currPerf)) {
                lastActivityMsSinceEpoch = now;
            }

            if (options.isStreaming() && !waitingForShutdown) {
                Duration quietFor = new Duration(lastActivityMsSinceEpoch, now);
                long fatalCount = new MetricsReader(job, query.getName()).getCounterMetric("fatal");

                if (fatalCount == -1) {
                    fatalCount = 0;
                }

                if (fatalCount > 0) {
                    NexmarkUtils.console("ERROR: job has fatal errors, cancelling.");
                    errors.add(String.format("Pipeline reported %s fatal errors", fatalCount));
                    waitingForShutdown = true;
                    cancelJob = true;
                } else if (configuration.debug
                        && configuration.numEvents > 0
                        && currPerf.numEvents == configuration.numEvents
                        && currPerf.numResults >= 0
                        && quietFor.isLongerThan(DONE_DELAY)) {
                    NexmarkUtils.console("streaming query appears to have finished waiting for completion.");
                    waitingForShutdown = true;
                } else if (quietFor.isLongerThan(STUCK_TERMINATE_DELAY)) {
                    NexmarkUtils.console(
                            "ERROR: streaming query appears to have been stuck for %d minutes, cancelling job.",
                            quietFor.getStandardMinutes());
                    errors.add(
                            String.format(
                                    "Cancelling streaming job since it appeared stuck for %d min.",
                                    quietFor.getStandardMinutes()));
                    waitingForShutdown = true;
                    cancelJob = true;
                } else if (quietFor.isLongerThan(STUCK_WARNING_DELAY)) {
                    NexmarkUtils.console(
                            "WARNING: streaming query appears to have been stuck for %d min.",
                            quietFor.getStandardMinutes());
                }

                if (cancelJob) {
                    try {
                        job.cancel();
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to cancel main job: ", e);
                    }
                }
            }

            perf = currPerf;

            boolean running = true;
            switch (state) {
                case UNKNOWN:
                case STOPPED:
                case RUNNING:
                    // Keep going.
                    break;
                case DONE:
                    // All done.
                    running = false;
                    break;
                case CANCELLED:
                    running = false;
                    if (!cancelJob) {
                        errors.add("Job was unexpectedly cancelled");
                    }
                    break;
                case FAILED:
                case UPDATED:
                    // Abnormal termination.
                    running = false;
                    errors.add("Job was unexpectedly updated");
                    break;
            }

            if (!running) {
                break;
            }

            if (lastActivityMsSinceEpoch == now) {
                NexmarkUtils.console("new perf %s", perf);
            } else {
                NexmarkUtils.console("no activity");
            }

            try {
                Thread.sleep(PERF_DELAY.getMillis());
            } catch (InterruptedException e) {
                Thread.interrupted();
                NexmarkUtils.console("Interrupted: pipeline is still running");
            }
        }

        perf.errors = errors;
        perf.snapshots = snapshots;

        if (publisherResult != null) {
            NexmarkUtils.console("Shutting down publisher pipeline.");
            try {
                if (!publisherCancelled) {
                    publisherJob.cancel();
                }
                publisherJob.waitUntilFinish(Duration.standardMinutes(5));
            } catch (IOException e) {
                throw new RuntimeException("Unable to cancel publisher job: ", e);
            }
        }

        return perf;
    }

    // ================================================================================
    // Basic sources and sinks
    // ================================================================================

    /**
     * Return a topic name.
     */
    private String shortTopic(long now) {
        String baseTopic = options.getPubsubTopic();
        if (Strings.isNullOrEmpty(baseTopic)) {
            throw new RuntimeException("Missing --pubsubTopic");
        }
        switch (options.getResourceNameMode()) {
            case VERBATIM:
                return baseTopic;
            case QUERY:
                return String.format("%s_%s_source", baseTopic, queryName);
            case QUERY_AND_SALT:
                return String.format("%s_%s_%d_source", baseTopic, queryName, now);
            case QUERY_RUNNER_AND_MODE:
                return String.format(
                        "%s_%s_%s_%s_source",
                        baseTopic, queryName, options.getRunner().getSimpleName(), options.isStreaming());
        }
        throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
    }

    /**
     * Return a subscription name.
     */
    private String shortSubscription(long now) {
        String baseSubscription = options.getPubsubSubscription();
        if (Strings.isNullOrEmpty(baseSubscription)) {
            throw new RuntimeException("Missing --pubsubSubscription");
        }
        switch (options.getResourceNameMode()) {
            case VERBATIM:
                return baseSubscription;
            case QUERY:
                return String.format("%s_%s_source", baseSubscription, queryName);
            case QUERY_AND_SALT:
                return String.format("%s_%s_%d_source", baseSubscription, queryName, now);
            case QUERY_RUNNER_AND_MODE:
                return String.format(
                        "%s_%s_%s_%s_source",
                        baseSubscription,
                        queryName,
                        options.getRunner().getSimpleName(),
                        options.isStreaming());
        }
        throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
    }

    /**
     * Return a file name for plain text.
     */
    private String textFilename(long now) {
        String baseFilename = options.getOutputPath();
        if (Strings.isNullOrEmpty(baseFilename)) {
            throw new RuntimeException("Missing --outputPath");
        }
        switch (options.getResourceNameMode()) {
            case VERBATIM:
                return baseFilename;
            case QUERY:
                return String.format("%s/nexmark_%s.txt", baseFilename, queryName);
            case QUERY_AND_SALT:
                return String.format("%s/nexmark_%s_%d.txt", baseFilename, queryName, now);
            case QUERY_RUNNER_AND_MODE:
                return String.format(
                        "%s/nexmark_%s_%s_%s",
                        baseFilename, queryName, options.getRunner().getSimpleName(), options.isStreaming());
        }
        throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
    }

    /**
     * Return a directory for logs.
     */
    private String logsDir(long now) {
        String baseFilename = options.getOutputPath();
        if (Strings.isNullOrEmpty(baseFilename)) {
            throw new RuntimeException("Missing --outputPath");
        }
        switch (options.getResourceNameMode()) {
            case VERBATIM:
                return baseFilename;
            case QUERY:
                return String.format("%s/logs_%s", baseFilename, queryName);
            case QUERY_AND_SALT:
                return String.format("%s/logs_%s_%d", baseFilename, queryName, now);
            case QUERY_RUNNER_AND_MODE:
                return String.format(
                        "%s/logs_%s_%s_%s",
                        baseFilename, queryName, options.getRunner().getSimpleName(), options.isStreaming());
        }
        throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
    }

    /**
     * Return a source of synthetic events.
     */
    private PCollection<Event> sourceEventsFromSynthetic(Pipeline p) {
        if (isStreaming()) {
            NexmarkUtils.console("Generating %d events in streaming mode", configuration.numEvents);
            return p.apply(queryName + ".ReadUnbounded", NexmarkUtils.streamEventsSource(configuration));
        } else {
            NexmarkUtils.console("Generating %d events in batch mode", configuration.numEvents);
            return p.apply(queryName + ".ReadBounded", NexmarkUtils.batchEventsSource(configuration));
        }
    }


    /**
     * Return Avro source of events from {@code options.getInputFilePrefix}.
     */
    private PCollection<Event> sourceEventsFromAvro(Pipeline p) {
        String filename = options.getInputPath();
        if (Strings.isNullOrEmpty(filename)) {
            throw new RuntimeException("Missing --inputPath");
        }
        NexmarkUtils.console("Reading events from Avro files at %s", filename);
        return p.apply(
                queryName + ".ReadAvroEvents", AvroIO.read(Event.class).from(filename + "*.avro"))
                .apply("OutputWithTimestamp", NexmarkQueryUtil.EVENT_TIMESTAMP_FROM_DATA);
    }


    /**
     * Sink all raw Events in {@code source} to {@code options.getOutputPath}. This will configure the
     * job to write the following files:
     *
     * <ul>
     * <li>{@code $outputPath/event*.avro} All Event entities.
     * <li>{@code $outputPath/auction*.avro} Auction entities.
     * <li>{@code $outputPath/bid*.avro} Bid entities.
     * <li>{@code $outputPath/person*.avro} Person entities.
     * </ul>
     *
     * @param source A PCollection of events.
     */
    private void sinkEventsToAvro(PCollection<Event> source) {
        String filename = options.getOutputPath();
        if (Strings.isNullOrEmpty(filename)) {
            throw new RuntimeException("Missing --outputPath");
        }
        NexmarkUtils.console("Writing events to Avro files at %s", filename);
        source.apply(
                queryName + ".WriteAvroEvents",
                AvroIO.write(Event.class).to(filename + "/event").withSuffix(".avro"));
        source
                .apply(NexmarkQueryUtil.JUST_BIDS)
                .apply(
                        queryName + ".WriteAvroBids",
                        AvroIO.write(Bid.class).to(filename + "/bid").withSuffix(".avro"));
        source
                .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)
                .apply(
                        queryName + ".WriteAvroAuctions",
                        AvroIO.write(Auction.class).to(filename + "/auction").withSuffix(".avro"));
        source
                .apply(NexmarkQueryUtil.JUST_NEW_PERSONS)
                .apply(
                        queryName + ".WriteAvroPeople",
                        AvroIO.write(Person.class).to(filename + "/person").withSuffix(".avro"));
    }

    /**
     * Send {@code formattedResults} to text files.
     */
    private void sinkResultsToText(PCollection<String> formattedResults, long now) {
        String filename = textFilename(now);
        NexmarkUtils.console("Writing results to text files at %s", filename);
        formattedResults.apply(queryName + ".WriteTextResults", TextIO.write().to(filename));
    }


    // ================================================================================
    // Construct overall pipeline
    // ================================================================================

    /**
     * Return source of events for this run, or null if we are simply publishing events to Pubsub.
     */
    private PCollection<Event> createSource(Pipeline p, final Instant now) throws IOException {
        PCollection<Event> source = null;

        switch (configuration.sourceType) {
            case DIRECT:
                source = sourceEventsFromSynthetic(p);
                break;
            case AVRO:
                source = sourceEventsFromAvro(p);
                break;
        }
        return source;
    }

    private static final TupleTag<String> MAIN = new TupleTag<String>() {
    };
    private static final TupleTag<String> SIDE = new TupleTag<String>() {
    };

    private static class PartitionDoFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().hashCode() % 2 == 0) {
                c.output(c.element());
            } else {
                c.output(SIDE, c.element());
            }
        }
    }

    /**
     * Consume {@code results}.
     */
    private void sink(PCollection<TimestampedValue<KnownSize>> results, long now) {
        if (configuration.sinkType == NexmarkUtils.SinkType.COUNT_ONLY) {
            // Avoid the cost of formatting the results.
            results.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
            return;
        }

        PCollection<String> formattedResults =
                results.apply(queryName + ".Format", NexmarkUtils.format(queryName));
        if (options.getLogResults()) {
            formattedResults =
                    formattedResults.apply(
                            queryName + ".Results.Log", NexmarkUtils.log(queryName + ".Results"));
        }

        switch (configuration.sinkType) {
            case DEVNULL:
                // Discard all results
                formattedResults.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
                break;
            case TEXT:
                sinkResultsToText(formattedResults, now);
                break;
            case AVRO:
                NexmarkUtils.console(
                        "WARNING: with --sinkType=AVRO, actual query results will be discarded.");
                break;
            case COUNT_ONLY:
                // Short-circuited above.
                throw new RuntimeException();
        }
    }

    // ================================================================================
    // Entry point
    // ================================================================================

    /**
     * Calculate the distribution of the expected rate of results per minute (in event time, not
     * wallclock time).
     */
    private void modelResultRates(NexmarkQueryModel model) {
        List<Long> counts = Lists.newArrayList(model.simulator().resultsPerWindow());
        Collections.sort(counts);
        int n = counts.size();
        if (n < 5) {
            NexmarkUtils.console("Query%s: only %d samples", model.configuration.query, n);
        } else {
            NexmarkUtils.console(
                    "Query%d: N:%d; min:%d; 1st%%:%d; mean:%d; 3rd%%:%d; max:%d",
                    model.configuration.query,
                    n,
                    counts.get(0),
                    counts.get(n / 4),
                    counts.get(n / 2),
                    counts.get(n - 1 - n / 4),
                    counts.get(n - 1));
        }
    }

    /**
     * Run {@code configuration} and return its performance if possible.
     */
    @Nullable
    public NexmarkPerf run() throws IOException {
        if (options.getManageResources() && !options.getMonitorJobs()) {
            throw new RuntimeException("If using --manageResources then must also use --monitorJobs.");
        }

        //
        // Setup per-run state.
        //
        checkState(queryName == null);


        try {
            NexmarkUtils.console("Running %s", configuration.toShortString());

            if (configuration.numEvents < 0) {
                NexmarkUtils.console("skipping since configuration is disabled");
                return null;
            }

            NexmarkQuery<? extends KnownSize> query = getNexmarkQuery();
            if (query == null) {
                NexmarkUtils.console("skipping since configuration is not implemented");
                return null;
            }

            queryName = query.getName();

            // Append queryName to temp location
            if (!"".equals(options.getTempLocation())) {
                options.setTempLocation(options.getTempLocation() + "/" + queryName);
            }

            NexmarkQueryModel model = getNexmarkQueryModel();

            if (options.getJustModelResultRate()) {
                if (model == null) {
                    throw new RuntimeException(String.format("No model for %s", queryName));
                }
                modelResultRates(model);
                return null;
            }

            final Instant now = Instant.now();
            Pipeline p = Pipeline.create(options);
            NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

            // Generate events.
            PCollection<Event> source = createSource(p, now);

            if (query.getTransform().needsSideInput()) {
                query.getTransform().setSideInput(NexmarkUtils.prepareSideInput(p, configuration));
            }

            if (options.getLogEvents()) {
                source = source.apply(queryName + ".Events.Log", NexmarkUtils.log(queryName + ".Events"));
            }

            // Source will be null if source type is PUBSUB and mode is PUBLISH_ONLY.
            // In that case there's nothing more to add to pipeline.
            if (source != null) {
                // Optionally sink events in Avro format.
                // (Query results are ignored).
                if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
                    sinkEventsToAvro(source);
                }


                // Apply query.
                PCollection<TimestampedValue<KnownSize>> results =
                        (PCollection<TimestampedValue<KnownSize>>) source.apply(query);

                if (options.getAssertCorrectness()) {
                    if (model == null) {
                        throw new RuntimeException(String.format("No model for %s", queryName));
                    }
                    // We know all our streams have a finite number of elements.
                    results.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
                    // If we have a finite number of events then assert our pipeline's
                    // results match those of a model using the same sequence of events.
                    PAssert.that(results).satisfies(model.assertionFor());
                }

                // Output results.
                sink(results, now.getMillis());
            }

            mainResult = p.run();
            mainResult.waitUntilFinish(Duration.standardSeconds(configuration.streamTimeout));
            return monitor(query);
        } finally {

            configuration = null;
            queryName = null;
        }
    }

    private boolean isSql() {
        return SQL.equalsIgnoreCase(options.getQueryLanguage());
    }

    private NexmarkQueryModel getNexmarkQueryModel() {
        Map<NexmarkQueryName, NexmarkQueryModel> models = createQueryModels();
        return models.get(configuration.query);
    }

    private NexmarkQuery<?> getNexmarkQuery() {
        Map<NexmarkQueryName, NexmarkQuery> queries = createQueries();
        return queries.get(configuration.query);
    }

    private Map<NexmarkQueryName, NexmarkQueryModel> createQueryModels() {
        return isSql() ? createSqlQueryModels() : createJavaQueryModels();
    }

    private Map<NexmarkQueryName, NexmarkQueryModel> createSqlQueryModels() {
        return ImmutableMap.of();
    }

    @Deprecated
    private Map<NexmarkQueryName, NexmarkQueryModel> createJavaQueryModels() {
        return null;
    }

    private Map<NexmarkQueryName, NexmarkQuery> createQueries() {
        return createSqlQueries();
    }

    //TODO : implement 12 sql queries
    private Map<NexmarkQueryName, NexmarkQuery> createSqlQueries() {
        return null;
    }


}
