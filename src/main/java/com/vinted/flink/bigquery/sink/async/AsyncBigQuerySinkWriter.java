package com.vinted.flink.bigquery.sink.async;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.vinted.flink.bigquery.metric.AsyncBigQueryStreamMetrics;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AsyncBigQuerySinkWriter<A> extends AsyncSinkWriter<Rows<A>, StreamRequest> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkWriter.class);
    private final AsyncClientProvider clientProvider;

    private final SinkWriterMetricGroup metricGroup;

    private final transient Map<String, AsyncBigQueryStreamMetrics> metrics = new HashMap<>();

    private final Executor appendExecutor;

    protected transient Map<String, StreamWriter> streamMap = new ConcurrentHashMap<>();

    public AsyncBigQuerySinkWriter(ExecutorProvider executorProvider, AsyncClientProvider clientProvider, ElementConverter<Rows<A>, StreamRequest> elementConverter, Sink.InitContext context, AsyncSinkWriterConfiguration configuration, Collection<BufferedRequestState<StreamRequest>> bufferedRequestStates) {
        super(elementConverter, context, configuration, bufferedRequestStates);
        appendExecutor = executorProvider.create();
        this.metricGroup = context.metricGroup();
        this.clientProvider = clientProvider;
    }

    private void registerInflightMetric(StreamWriter writer) {
        var group = metricGroup
                .addGroup("stream", writer.getStreamName())
                .addGroup("writer_id", writer.getWriterId());

        group.gauge("inflight_wait_seconds", writer::getInflightWaitSeconds);
    }

    private void registerAppendMetrics(StreamRequest request) {
        metrics.computeIfAbsent(request.getStream(), s -> {
            var metric = new AsyncBigQueryStreamMetrics();
            var group = metricGroup
                    .addGroup("table", request.getTable().getTable())
                    .addGroup("stream", request.getStream());
            group.gauge("batch_count", metric::getBatchCount);
            group.gauge("batch_size_mb", metric::getBatchSizeInMb);
            group.gauge("split_batch_count", metric::getSplitBatchCount);
            group.gauge("callback_timeouts", metric::getTimeoutCount);
            group.gauge("inflight_requests", metric::getInflightRequests);

            return metric;
        });
    }

    protected final StreamWriter streamWriter(String traceId, String streamName, TableId table) {
        var writer = streamMap.computeIfAbsent(streamName, name -> {
            logger.trace("Trace-id {} Stream not found {}. Creating new stream", traceId, streamName);

            var newWriter = this.clientProvider.getWriter(streamName, table);
            registerInflightMetric(newWriter);
            return newWriter;
        });

        if (writer.isClosed() || writer.isUserClosed()) {
            logger.warn("Trace-id {}, StreamWrite is closed. Recreating stream for {}", traceId, streamName);
            recreateStreamWriter(traceId, streamName, writer.getWriterId(), table);
            return streamMap.get(streamName);
        }

        return writer;
    }

    protected final void recreateStreamWriter(String traceId, String streamName, String writerId, TableId table) {
        logger.info("Trace-id {} Closing all writers for {}", traceId, streamName);
        streamMap.replaceAll((key, writer) -> {
            var newWriter = writer;
            if (writer.getWriterId().equals(writerId)) {
                try {
                    writer.close();
                } catch (Exception e) {
                    logger.trace("Trace-id {} Could not close writer for {}", traceId, streamName);
                }
                newWriter = this.clientProvider.getWriter(streamName, table);
                registerInflightMetric(newWriter);
            }
            return newWriter;
        });
    }

    @Override
    protected void submitRequestEntries(List<StreamRequest> list, Consumer<List<StreamRequest>> consumer) {
        var traceId = UUID.randomUUID().toString();
        var requests = list.stream().map(request -> {
            registerAppendMetrics(request);
            var writer = streamWriter(traceId, request.getStream(), request.getTable());
            logger.trace("Trace-id {}, Writing rows stream {} to steamWriter for {} writer id {}", traceId, request.getStream(), writer.getStreamName(), writer.getWriterId());
            return CompletableFuture.<List<StreamRequest>>supplyAsync(() -> {
                        try {
                            Optional.ofNullable(metrics.get(request.getStream())).ifPresent(s -> {
                                s.updateSize(request.getData().getSerializedSize());
                                s.setBatchCount(request.getData().getSerializedRowsCount());
                                s.incrementInflightRequests();
                            });
                            writer.append(request.getData()).get();
                            return List.of();
                        } catch (Throwable t) {
                            Optional.ofNullable(metrics.get(request.getStream())).ifPresent(s -> {
                                s.updateSize(request.getData().getSerializedSize());
                                s.setBatchCount(request.getData().getSerializedRowsCount());
                                s.decrementInflightRequests();
                            });
                            logger.error("Trace-id {}, StreamWriter failed to append {}", traceId, t.getMessage());
                            var status = Status.fromThrowable(t);
                            switch (status.getCode()) {
                                case UNAVAILABLE: {
                                    this.recreateStreamWriter(traceId, request.getStream(), writer.getWriterId(), request.getTable());
                                    return retry(t, traceId, request);
                                }
                                case INVALID_ARGUMENT:
                                    if (t.getMessage().contains("INVALID_ARGUMENT: MessageSize is too large.")) {
                                        Optional.ofNullable(this.metrics.get(request.getStream())).ifPresent(AsyncBigQueryStreamMetrics::incSplitCount);
                                        logger.warn("Trace-id {} MessageSize is too large. Splitting batch", traceId);
                                        var data = request.getData().getSerializedRowsList();
                                        var first = data.subList(0, data.size() / 2);
                                        var second = data.subList(data.size() / 2, data.size());
                                        try {
                                            return List.of(
                                                    new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(first).build(), request.getRetries() - 1),
                                                    new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(second).build(), request.getRetries() - 1)
                                            );
                                        } catch (Throwable e) {
                                            throw new AsyncWriterException(traceId, status.getCode(), e);
                                        }
                                    } else {
                                        logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                                        throw new AsyncWriterException(traceId, status.getCode(), t);
                                    }
                                case UNKNOWN:
                                    if (status.getCause() instanceof Exceptions.MaximumRequestCallbackWaitTimeExceededException) {
                                        logger.info("Trace-id {} request timed out: {}", traceId, t.getMessage());
                                        Optional.ofNullable(this.metrics.get(request.getStream()))
                                                .ifPresent(AsyncBigQueryStreamMetrics::incrementTimeoutCount);
                                        this.recreateStreamWriter(traceId, request.getStream(), writer.getWriterId(), request.getTable());
                                        return retry(t, traceId, request);
                                    } else {
                                        logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                                        throw new AsyncWriterException(traceId, status.getCode(), t);
                                    }
                                default:
                                    logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                                    throw new AsyncWriterException(traceId, status.getCode(), t);
                            }

                        }
                    }, appendExecutor)
                    .exceptionally(t -> {
                        var status = Status.fromThrowable(t);
                        this.getFatalExceptionCons().accept(new AsyncWriterException(traceId, status.getCode(), t));
                        return List.of(request);
                    })
                    .whenCompleteAsync((v, e) -> {
                        Optional.ofNullable(this.metrics.get(request.getStream()))
                                .ifPresent(AsyncBigQueryStreamMetrics::decrementInflightRequests);
                    }, appendExecutor);
        }).collect(Collectors.toList());

        CompletableFuture
                .allOf(requests.toArray(new CompletableFuture[0]))
                .thenApplyAsync(v -> requests.stream().flatMap(s -> s.join().stream()).collect(Collectors.toList()), appendExecutor)
                .whenComplete((result, e) -> {
                    if (e != null) {
                        var status = Status.fromThrowable(e);
                        this.getFatalExceptionCons().accept(new AsyncWriterException(traceId, status.getCode(), e));
                    } else {
                        consumer.accept(result);
                    }
                });

    }

    private List<StreamRequest> retry(Throwable t, String traceId, StreamRequest request) {
        var status = Status.fromThrowable(t);
        request.setRetries(request.getRetries() - 1);
        if (request.getRetries() > 0) {
            logger.warn("Trace-id {} Recoverable error {}. Retrying {} ...", traceId, status.getCode(), request.getRetries());
            return List.of(request);
        } else {
            logger.error("Trace-id {} Recoverable error {}. No more retries left", traceId, status.getCode(), t);
            this.getFatalExceptionCons().accept(new AsyncWriterException(traceId, status.getCode(), t));
            return List.of();
        }
    }

    @Override
    protected long getSizeInBytes(StreamRequest StreamRequest) {
        return StreamRequest.getData().getSerializedSize();
    }

    @Override
    public void close() {
        logger.info("Closing BigQuery write stream");
        try {
            flush(true);
            streamMap.values().forEach(stream -> {
                try {
                    stream.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }


}
