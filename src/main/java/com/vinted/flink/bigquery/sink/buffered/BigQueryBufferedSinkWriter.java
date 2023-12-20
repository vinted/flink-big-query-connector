package com.vinted.flink.bigquery.sink.buffered;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.AppendException;
import com.vinted.flink.bigquery.sink.BigQuerySinkWriter;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public abstract class BigQueryBufferedSinkWriter<A, StreamT extends AutoCloseable>
        extends BigQuerySinkWriter<A, StreamT>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<Rows<A>, BigQueryCommittable> {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryBufferedSinkWriter.class);
    private Map<String, Long> streamOffsets = new ConcurrentHashMap<>();

    public BigQueryBufferedSinkWriter(
            Sink.InitContext sinkInitContext,
            RowValueSerializer<A> rowSerializer,
            ClientProvider clientProvider,
            ExecutorProvider executorProvider) {
        super(sinkInitContext, rowSerializer, clientProvider, executorProvider);
    }

    protected abstract ApiFuture<AppendRowsResponse> append(String traceId, Rows<A> rows);

    @Override
    protected void writeWithRetry(String traceId, Rows<A> rows, int retryCount) throws Throwable {
        try {
            logger.debug(
                    "Trace-id: {} Appending rows \nstream: {}\ntable: {}\noffset: {}\nsize: {}\nretries: {}",
                    traceId, rows.getStream(), rows.getTable(), rows.getOffset(), rows.getData().size(), retryCount
            );
            var response = append(traceId, rows);
            var callback = new AppendCallBack<>(this, rows, retryCount, traceId);
            ApiFutures.addCallback(response, callback, appendExecutor);
            try {
                callback.future.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        } catch (AppendException exception) {
            var error = exception.getError();
            var errorRows = exception.<A>getRows();
            var errorTraceId = exception.getTraceId();
            var status = Status.fromThrowable(error);
            Function<String, String> createLogMessage = (title) ->
                    this.createLogMessage(title, errorTraceId, status, error, errorRows);
            switch (status.getCode()) {
                case INTERNAL:
                case CANCELLED:
                case ABORTED: {
                    logger.warn(createLogMessage.apply("Recoverable error. Retrying.., "), error);
                    try {
                        Thread.sleep(clientProvider.writeSettings().getRetryPause().toMillis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    if (retryCount > 0) {
                        writeWithRetry(errorTraceId, errorRows, retryCount - 1);
                    } else {
                        throw error;
                    }
                    break;
                }
                // ALREADY_EXISTS: The row was already written.
                // This error can happen when you provide stream offsets.
                // It indicates that a duplicate record was detected.
                // It's caused by retrying some batch in case more rows are flushed and offset commit is not stored in checkpoint.
                // Sometimes whole batch can be skipped, sometimes only part of the batch is skipped and new recores are appended.
                case ALREADY_EXISTS: {
                    if (error instanceof Exceptions.OffsetAlreadyExists) {
                        var o = (Exceptions.OffsetAlreadyExists) error;
                        var offsetToSkip = (int) (o.getExpectedOffset() - o.getActualOffset());

                        if (offsetToSkip >= errorRows.getData().size()) {
                            logger.info(
                                    createLogMessage.apply("Whole batch was already stored. Expected offset {}, skipping..."), o.getExpectedOffset()
                            );
                        } else {
                            var batchToStore = errorRows.getData().subList(offsetToSkip, errorRows.getData().size());
                            logger.warn(
                                    createLogMessage.apply("Skipping {} items from batch. Offsets:({}-{})"),
                                    offsetToSkip, o.getExpectedOffset(), o.getActualOffset()
                            );
                            logger.info(createLogMessage.apply("Storing {} items with offset {}"), batchToStore.size(), o.getExpectedOffset());
                            writeWithRetry(errorTraceId, errorRows.updateBatch(batchToStore, o.getExpectedOffset()), clientProvider.writeSettings().getRetryCount());
                        }
                    } else {
                        logger.error(
                                createLogMessage.apply("Unable to parse expected and actual offset. Failed to write this batch."), error);
                        throw error;
                    }

                    break;
                }
                // OUT_OF_RANGE Returned when the specified offset in the stream is beyond the current end of the stream.
                // This is non recoverable exception. Wrapping message with debug info and throwing it.
                case OUT_OF_RANGE: {
                    if (error instanceof Exceptions.OffsetOutOfRange) {
                        var o = (Exceptions.OffsetOutOfRange) error;
                        logger.error(createLogMessage.apply("Actual offset " + o.getActualOffset() + " is out range. Expected " + o.getExpectedOffset()), error);
                    } else {
                        logger.error(createLogMessage.apply(error.getMessage()), error);
                    }

                    throw error;
                }
                // INVALID_ARGUMENT Stream is already finalized.
                case INVALID_ARGUMENT: {
                    if (error instanceof Exceptions.StreamFinalizedException) {
                        logger.error(createLogMessage.apply(
                                "Stream is already  finalized actualOffset: {}."), errorRows.getOffset(), error);
                        throw error;
                    } else if (error.getMessage().contains("INVALID_ARGUMENT: MessageSize is too large.")) {
                        Optional.ofNullable(metrics.get(errorRows.getStream())).ifPresent(BigQueryStreamMetrics::incSplitCount);
                        logger.warn(createLogMessage.apply("MessageSize is too large. Splitting batch"));
                        var first = errorRows.getData().subList(0, errorRows.getData().size() / 2);
                        var second = errorRows.getData().subList(errorRows.getData().size() / 2, errorRows.getData().size());
                        writeWithRetry(errorTraceId, errorRows.updateBatch(first, errorRows.getOffset()), clientProvider.writeSettings().getRetryCount());
                        writeWithRetry(errorTraceId, errorRows.updateBatch(second, errorRows.getOffset() + first.size()), clientProvider.writeSettings().getRetryCount());
                    } else {
                        logger.error(createLogMessage.apply(error.getMessage()), error);
                        throw error;
                    }
                    break;
                }
                default: {
                    logger.error(createLogMessage.apply("Non recoverable BigQuery stream error for:"), error);
                    throw error;
                }
            }
        } catch (Throwable t) {
            logger.error("Non recoverable BigQuery stream error for:", t);
            throw t;
        }

    }

    @Override
    public List<BigQueryCommittable> prepareCommit() {
        var result = new ArrayList<BigQueryCommittable>();
        streamOffsets.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .forEach(entry -> {
                    result.add(new BigQueryCommittable(entry.getKey(), entry.getValue()));
                });
        streamOffsets.clear();
        return result;
    }

    @Override
    public void close() {
        logger.info("Closing BigQuery write stream");
        streamMap.values().forEach(stream -> {
            try {
                stream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        client.close();
    }

    @Override
    public void flush(boolean endOfInput) {
    }

    static class AppendCallBack<A> implements ApiFutureCallback<AppendRowsResponse> {
        private final BigQueryBufferedSinkWriter<?, ?> parent;
        private final Rows<A> rows;
        private final String traceId;
        private final int retryCount;

        private final CompletableFuture<AppendRowsResponse> future = new CompletableFuture<>();

        public AppendCallBack(BigQueryBufferedSinkWriter<?, ?> parent, Rows<A> rows, int retryCount, String traceId) {
            this.parent = parent;
            this.rows = rows;
            this.traceId = traceId;
            this.retryCount = retryCount;
        }

        @Override
        public void onFailure(Throwable t) {
            logger.info("Trace-id {} Received error {}", t.getMessage(), traceId);
            future.completeExceptionally(new AppendException(traceId, rows, retryCount, t));
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
            this.parent.streamOffsets.put(rows.getStream(), rows.getOffset() + rows.getData().size());
            var streamOffset = this.parent.streamOffsets.get(rows.getStream());
            logger.debug(
                    "Trace-id {} Stream offset updated \nstream: {}\ntable: {}\nnew offset: {}\nsize: {}\nretries: {}\nresponse offset: {}",
                    this.traceId, rows.getStream(), rows.getTable(), streamOffset, rows.getData().size(), retryCount, result.getAppendResult().getOffset()
            );
            Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(m -> {
                m.setBatchCount(rows.getData().size());
                m.setOffset(rows.getOffset());
            });
            future.complete(result);
        }
    }
}
