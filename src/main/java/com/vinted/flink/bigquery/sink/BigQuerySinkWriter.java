package com.vinted.flink.bigquery.sink;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.common.collect.Iterators;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BigQuerySinkWriter<A, StreamT extends AutoCloseable> implements SinkWriter<Rows<A>> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Iterator<Integer> streamIndexIterator;
    private final SinkWriterMetricGroup metricGroup;

    protected ClientProvider<StreamT> clientProvider;
    protected transient Map<String, StreamT> streamMap = new ConcurrentHashMap<>();
    protected Sink.InitContext sinkInitContext;
    protected RowValueSerializer<A> rowSerializer;

    protected BigQueryWriteClient client;
    protected final Executor appendExecutor;
    protected Counter numBytesOutCounter;
    protected Counter numRecordsOutCounter;
    protected transient Map<String, BigQueryStreamMetrics> metrics = new HashMap<>();

    protected abstract ApiFuture<AppendRowsResponse> append(String traceId, Rows<A> rows);

    public BigQuerySinkWriter(
            Sink.InitContext sinkInitContext,
            RowValueSerializer<A> rowSerializer,
            ClientProvider<StreamT> clientProvider,
            ExecutorProvider executorProvider) {

        this.sinkInitContext = sinkInitContext;
        this.rowSerializer = rowSerializer;
        this.clientProvider = clientProvider;
        this.appendExecutor = executorProvider.create();
        this.client = this.clientProvider.getClient();
        this.metricGroup = this.sinkInitContext.metricGroup();
        this.numBytesOutCounter = this.metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = this.metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.streamIndexIterator = Iterators.cycle(IntStream
                .range(0, this.clientProvider.writeSettings().getStreamsPerTable())
                .boxed()
                .collect(Collectors.toList()));

    }

    protected final StreamT streamWriter(String traceId, String streamName, TableId table) {
        var streamWithIndex = String.format("%s-%s",streamName, streamIndexIterator.next());
        return streamMap.computeIfAbsent(streamWithIndex, name -> {
            logger.trace("Trace-id {} Stream not found {}. Creating new stream", traceId, streamWithIndex);
            // Stream name can't contain index
            return this.clientProvider.getWriter(streamName, table);
        });
    }

    @Override
    public void write(Rows<A> rows, Context context) {
        numRecordsOutCounter.inc(rows.getData().size());
        metrics.computeIfAbsent(rows.getStream(), s -> {
            var metric = new BigQueryStreamMetrics(rows.getStream());
            var group = metricGroup
                    .addGroup("table", rows.getTable().getTable())
                    .addGroup("stream", rows.getStream());
            group.gauge("stream_offset", (Gauge<Long>) metric::getOffset);
            group.gauge("batch_count", metric::getBatchCount);
            group.gauge("batch_size_mb", metric::getBatchSizeInMb);
            group.gauge("split_batch_count", metric::getSplitBatchCount);

            return metric;
        });
        var traceId = UUID.randomUUID().toString();
        try {
            writeWithRetry(traceId, rows, clientProvider.writeSettings().getRetryCount());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void writeWithRetry(String traceId, Rows<A> rows, int retryCount) throws Throwable;

    protected String createLogMessage(String title, String errorTraceId, Status status, Throwable error, Rows<A> errorRows) {
        return String.format("Trace-id: %s %s \nstatus: %s\nerror: %s\nstream: %s\ntable: %s\nactual offset: %s\nsize: %s",
                errorTraceId,
                title,
                status.getCode(),
                error.getMessage(),
                errorRows.getStream(),
                errorRows.getTable(),
                errorRows.getOffset(),
                errorRows.getData().size()
        );
    }

}
