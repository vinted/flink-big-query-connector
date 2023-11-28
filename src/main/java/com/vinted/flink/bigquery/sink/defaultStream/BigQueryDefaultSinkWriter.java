package com.vinted.flink.bigquery.sink.defaultStream;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.AppendException;
import com.vinted.flink.bigquery.sink.BigQuerySinkWriter;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.function.Function;

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
    }

    private void checkAsyncException() {
        // reset this exception since we could close the writer later on
        RuntimeException e = appendAsyncException;
        if (e != null) {
            appendAsyncException = null;
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
            Function<String, String> createLogMessage = (title) ->
                    this.createLogMessage(title, errorTraceId, status, error, errorRows);
            logger.error(createLogMessage.apply("Non recoverable BigQuery stream error for:"), error);
            throw error;
        } catch (Throwable t) {
            logger.error("Non recoverable BigQuery stream error for:", t);
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
        if (endOfInput) {
            inflightRequestCount.arriveAndAwaitAdvance();
        }
        checkAsyncException();
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
        public void onFailure(Throwable t) {
            logger.info("Trace-id {} Received error {}", t.getMessage(), traceId);
            this.parent.inflightRequestCount.arriveAndDeregister();
            var status = Status.fromThrowable(t);
            if (status.getCode() == Status.Code.INVALID_ARGUMENT && t.getMessage().contains("INVALID_ARGUMENT: MessageSize is too large.")) {
                Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(BigQueryStreamMetrics::incSplitCount);
                logger.warn("Trace-id {} MessageSize is too large. Splitting batch", traceId);
                var first = rows.getData().subList(0, rows.getData().size() / 2);
                var second = rows.getData().subList(rows.getData().size() / 2, rows.getData().size());
                try {
                    this.parent.writeWithRetry(traceId, rows.updateBatch(first, rows.getOffset()), retryCount);
                    this.parent.writeWithRetry(traceId, rows.updateBatch(second, rows.getOffset() + first.size()), retryCount);
                } catch (Throwable e) {
                    this.parent.appendAsyncException = new AppendException(traceId, rows, t);
                }
            } else {
                this.parent.appendAsyncException = new AppendException(traceId, rows, t);
            }
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
            Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(m -> {
                m.setBatchCount(rows.getData().size());
                m.setOffset(result.getAppendResult().getOffset().getValue());
            });
            this.parent.inflightRequestCount.arriveAndDeregister();
        }
    }
}
