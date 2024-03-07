package com.vinted.flink.bigquery.sink.defaultStream;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
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

public class BigQueryDefaultSinkWriter<A>
        extends BigQuerySinkWriter<A> {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryDefaultSinkWriter.class);

    private final Phaser inflightRequestCount = new Phaser(1);
    private volatile AppendException appendAsyncException = null;

    public BigQueryDefaultSinkWriter(
            Sink.InitContext sinkInitContext,
            RowValueSerializer<A> rowSerializer,
            ClientProvider<A> clientProvider,
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
            logger.error(this.createLogMessage("Non recoverable async BigQuery stream AppendException for:",  errorTraceId, status, error, errorRows, 0), error);
            throw e;
        }
    }

    protected AppendResult append(String traceId, Rows<A> rows) {
        var size = 0L;
        numRecordsOutCounter.inc(rows.getData().size());
        Optional.ofNullable(metrics.get(rows.getStream())).ifPresent(s -> s.updateSize(size));

        var writer = streamWriter(traceId, rows.getStream(), rows.getTable());

        if (writer.isClosed()) {
            logger.warn("Trace-id {}, StreamWrite is closed. Recreating stream for {}", traceId, rows.getStream());
            recreateStreamWriter(traceId, rows.getStream(), writer.getWriterId(), rows.getTable());
            writer = streamWriter(traceId, rows.getStream(), rows.getTable());
        }

        logger.trace("Trace-id {}, Writing rows stream {} to steamWriter for {} writer id {}", traceId, rows.getStream(), writer.getStreamName(), writer.getWriterId());
        try {
            return new AppendResult(writer.append(rows), writer.getWriterId());
        } catch (Throwable t) {
            logger.error("Trace-id {}, StreamWriter failed to append {}", traceId, t.getMessage());
            return AppendResult.failure(t, writer.getWriterId());
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
            var result = append(traceId, rows);
            var callback = new AppendCallBack<>(this, traceId, rows, retryCount);
            ApiFutures.addCallback(result.response, callback, appendExecutor);
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
        logger.debug("Flushing BigQuery writer {} data. Inflight request count {}", this.sinkInitContext.getSubtaskId(), inflightRequestCount.getRegisteredParties());
        checkAsyncException();
        inflightRequestCount.arriveAndAwaitAdvance();
        logger.debug("BigQuery writer {} data flushed. Inflight request count {}", this.sinkInitContext.getSubtaskId(), inflightRequestCount.getRegisteredParties());
    }

    static class AppendCallBack<A> implements ApiFutureCallback<AppendRowsResponse> {
        private final BigQueryDefaultSinkWriter<A> parent;
        private final Rows<A> rows;

        private final String traceId;

        private final int retryCount;

        public AppendCallBack(BigQueryDefaultSinkWriter<A> parent, String traceId, Rows<A> rows, int retryCount) {
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
            this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
            this.parent.inflightRequestCount.arriveAndDeregister();
        }
    }
}
