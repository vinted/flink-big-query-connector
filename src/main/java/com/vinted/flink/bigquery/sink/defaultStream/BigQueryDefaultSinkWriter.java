package com.vinted.flink.bigquery.sink.defaultStream;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.AppendException;
import com.vinted.flink.bigquery.sink.BigQuerySinkWriter;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Phaser;

public abstract class BigQueryDefaultSinkWriter<A, StreamT extends AutoCloseable>
        extends BigQuerySinkWriter<A, StreamT> {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryDefaultSinkWriter.class);

    private final Phaser inflightRequestCount = new Phaser(1);
    private volatile AppendException appendAsyncException = null;

    public BigQueryDefaultSinkWriter(
            Sink.InitContext sinkInitContext,
            RowValueSerializer<A> rowSerializer,
            ClientProvider<StreamT> clientProvider,
            ExecutorProvider executorProvider) {
        super(sinkInitContext, rowSerializer, clientProvider, executorProvider);

        var metricGroup =  this.sinkInitContext.metricGroup();
        var group = metricGroup
                .addGroup("BigQuery")
                .addGroup("DefaultSinkWriter")
                .addGroup("inflight_requests");

        group.gauge("count", this.inflightRequestCount::getRegisteredParties);
    }

    private void checkAsyncException() {
        // reset this exception since we could close the writer later on
        AppendException e = appendAsyncException;
        if (e != null) {
            appendAsyncException = null;
            var error = e.getError();
            var errorRows = e.<A>getRows();
            var errorTraceId = e.getTraceId();
            var status = Status.fromThrowable(error);
            logger.error(this.createLogMessage("Non recoverable BigQuery stream AppendException for:",  errorTraceId, status, error, errorRows, 0), error);
            throw e;
        }
    }

    @Override
    protected void writeWithRetry(String traceId, Rows<A> rows, int retryCount) throws Throwable {
        try {
            checkAsyncException();
            logger.debug(
                    "Trace-id: {} Appending rows \nstream: {}\ntable: {}\noffset: {}\nsize: {}\nretries: {}",
                    traceId, rows.getStream(), rows.getTable(), rows.getOffset(), rows.getData().size(), retryCount
            );
            var response = append(traceId, rows);
            var callback = new AppendCallBack<>(this, traceId, rows, retryCount);
            ApiFutures.addCallback(response, callback, appendExecutor);
            inflightRequestCount.register();
        } catch (AppendException exception) {
            var error = exception.getError();
            var errorRows = exception.<A>getRows();
            var errorTraceId = exception.getTraceId();
            var status = Status.fromThrowable(error);
            logger.error(this.createLogMessage("Non recoverable BigQuery stream AppendException for:",  errorTraceId, status, error, errorRows, retryCount), error);
            throw error;
        } catch (Throwable t) {
            logger.error("Trace-id: {} Non recoverable BigQuery stream error for: {}. Retry count: {}", traceId, t.getMessage(), retryCount);
            throw t;
        }
    }

    @Override
    public void close() {
        logger.info("Closing BigQuery write stream");
        inflightRequestCount.arriveAndAwaitAdvance();
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
        logger.info("Flushing BigQuery writer {} data. Inflight request count {}", this.sinkInitContext.getSubtaskId(), inflightRequestCount.getRegisteredParties());
        checkAsyncException();
        inflightRequestCount.arriveAndAwaitAdvance();
        logger.info("BigQuery writer {} data flushed. Inflight request count {}", this.sinkInitContext.getSubtaskId(), inflightRequestCount.getRegisteredParties());
    }

    static class AppendCallBack<A> implements ApiFutureCallback<AppendRowsResponse> {
        private final BigQueryDefaultSinkWriter<A, ?> parent;
        private final Rows<A> rows;
        private final String traceId;
        private final int retryCount;

        public AppendCallBack(BigQueryDefaultSinkWriter<A, ?> parent, String traceId, Rows<A> rows, int retryCount) {
            this.parent = parent;
            this.traceId = traceId;
            this.rows = rows;
            this.retryCount = retryCount;
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
            Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(m -> {
                m.setBatchCount(rows.getData().size());
                m.setOffset(result.getAppendResult().getOffset().getValue());
            });
            this.parent.inflightRequestCount.arriveAndDeregister();
        }


        @Override
        public void onFailure(Throwable t) {
            var status = Status.fromThrowable(t);
            switch (status.getCode()) {
                case INTERNAL:
                case ABORTED:
                case CANCELLED:
                case FAILED_PRECONDITION:
                case DEADLINE_EXCEEDED:
                    doPauseBeforeRetry();
                    retryWrite(t, retryCount - 1);
                    break;
                case UNAVAILABLE: {
                    this.parent.recreateAllStreamWriters(traceId, rows.getStream(), rows.getTable());
                    retryWrite(t, retryCount - 1);
                    break;
                }
                case INVALID_ARGUMENT:
                    if (t.getMessage().contains("INVALID_ARGUMENT: MessageSize is too large.")) {
                        Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(BigQueryStreamMetrics::incSplitCount);
                        logger.warn("Trace-id {} MessageSize is too large. Splitting batch", traceId);
                        var data = rows.getData();
                        var first = data.subList(0, data.size() / 2);
                        var second = data.subList(data.size() / 2, data.size());
                        try {
                            this.parent.writeWithRetry(traceId, rows.updateBatch(first, rows.getOffset()), retryCount - 1);
                            this.parent.writeWithRetry(traceId, rows.updateBatch(second, rows.getOffset() + first.size()), retryCount - 1);
                        } catch (Throwable e) {
                            this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
                        }
                    } else {
                        logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                        this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
                    }
                    break;
                default:
                    logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                    this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
            }
            this.parent.inflightRequestCount.arriveAndDeregister();
        }

        private void retryWrite(Throwable t, int newRetryCount) {
            var status = Status.fromThrowable(t);
            try {
                if (newRetryCount > 0) {
                    logger.warn("Trace-id {} Recoverable error {}. Retrying {} ...", traceId, status.getCode(), retryCount);
                    this.parent.writeWithRetry(traceId, rows, newRetryCount);
                } else {
                    logger.error("Trace-id {} Recoverable error {}. No more retries left", traceId, status.getCode(), t);
                    this.parent.appendAsyncException = new AppendException(traceId, rows, newRetryCount, t);
                }
            } catch (Throwable e) {
                this.parent.appendAsyncException = new AppendException(traceId, rows, newRetryCount, e);
            }
        }

        private void doPauseBeforeRetry() {
            try {
                Thread.sleep(parent.clientProvider.writeSettings().getRetryPause().toMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
