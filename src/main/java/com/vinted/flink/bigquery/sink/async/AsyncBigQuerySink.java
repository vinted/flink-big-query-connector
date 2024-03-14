package com.vinted.flink.bigquery.sink.async;

import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;

public class AsyncBigQuerySink<A> extends AsyncSinkBase<Rows<A>, StreamRequest> {
    private final AsyncClientProvider provider;
    private final RateLimitingStrategy strategy;

    private final ExecutorProvider executorProvider;

    public static <A> AsyncBigQuerySinkBuilder<A> builder() {
        return new AsyncBigQuerySinkBuilder<>();
    }

    protected AsyncBigQuerySink(ExecutorProvider executorProvider, AsyncClientProvider provider, RateLimitingStrategy rateLimitingStrategy, ElementConverter<Rows<A>, StreamRequest> elementConverter, int maxBatchSize, int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS, long maxRecordSizeInBytes) {
        super(elementConverter, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes);
        this.executorProvider = executorProvider;
        this.provider = provider;
        this.strategy = rateLimitingStrategy;
    }

    @Override
    public StatefulSinkWriter<Rows<A>, BufferedRequestState<StreamRequest>> createWriter(InitContext initContext) throws IOException {
        return new AsyncBigQuerySinkWriter<>(executorProvider, provider, this.getElementConverter(), initContext,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .setRateLimitingStrategy(strategy)
                        .build(),
                List.of()
        );
    }

    @Override
    public StatefulSinkWriter<Rows<A>, BufferedRequestState<StreamRequest>> restoreWriter(InitContext initContext, Collection<BufferedRequestState<StreamRequest>> collection) throws IOException {
        return new AsyncBigQuerySinkWriter<>(executorProvider, provider, this.getElementConverter(), initContext,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .setRateLimitingStrategy(strategy)
                        .build(),
                collection
        );
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<StreamRequest>> getWriterStateSerializer() {
        return new StreamRequestSerializer();
    }

    static public class AsyncBigQuerySinkBuilder<A> extends AsyncSinkBaseBuilder<Rows<A>, StreamRequest, AsyncBigQuerySinkBuilder<A>> {
        private static final int DEFAULT_MAX_BATCH_SIZE = 1;
        private static final int DEFAULT_IN_FLIGHT_REQUESTS = 4;
        private static final int DEFAULT_MAX_BUFFERED_REQUESTS = DEFAULT_MAX_BATCH_SIZE + 1;
        private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 500000000; //500MB

        private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = Duration.ofSeconds(10).toMillis();

        private static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 10000000;
        private AsyncClientProvider provider;

        private RowValueSerializer<A> serializer;

        private RateLimitingStrategy strategy = null;

        private ExecutorProvider executorProvider = () -> Executors.newFixedThreadPool(4);

        public AsyncBigQuerySinkBuilder<A> setClientProvider(AsyncClientProvider provider) {
            this.provider = provider;
            return this;
        }

        public AsyncBigQuerySinkBuilder<A> setRowSerializer(RowValueSerializer<A> serializer) {
            this.serializer = serializer;
            return this;
        }

        public AsyncBigQuerySinkBuilder<A> setRateLimitStrategy(RateLimitingStrategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public AsyncBigQuerySinkBuilder<A> setExecutorProvider(ExecutorProvider executorProvider) {
            this.executorProvider = executorProvider;
            return this;
        }

        @Override
        public AsyncSinkBase<Rows<A>, StreamRequest> build() {
            if (getMaxBatchSize() == null) {
                setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE);
            }

            if (getMaxInFlightRequests() == null) {
                setMaxInFlightRequests(DEFAULT_IN_FLIGHT_REQUESTS);
            }

            if (getMaxBufferedRequests() == null) {
                setMaxBufferedRequests(DEFAULT_MAX_BUFFERED_REQUESTS);
            }

            if (getMaxBatchSizeInBytes() == null) {
                setMaxBatchSizeInBytes(DEFAULT_MAX_BATCH_SIZE_IN_BYTES);
            }

            if (getMaxTimeInBufferMS() == null) {
                setMaxTimeInBufferMS(DEFAULT_MAX_TIME_IN_BUFFER_MS);
            }

            if (getMaxRecordSizeInBytes() == null) {
                setMaxRecordSizeInBytes(DEFAULT_MAX_RECORD_SIZE_IN_BYTES);
            }

            return new AsyncBigQuerySink<>(
                    this.executorProvider,
                    this.provider,
                    this.strategy,
                    new ProtoElementConverter<>(this.serializer, this.provider.writeSettings().getRetryCount()),
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBatchSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    getMaxRecordSizeInBytes()
            );
        }
    }
}
