package com.vinted.flink.bigquery.sink.async;

import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;

import java.time.Duration;

public class AsyncBigQuerySinkBuilder<A> extends AsyncSinkBaseBuilder<Rows<A>, StreamRequest, AsyncBigQuerySinkBuilder<A>> {
    private static final int DEFAULT_MAX_BATCH_SIZE = 1;
    private static final int DEFAULT_IN_FLIGHT_REQUESTS = 4;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = DEFAULT_MAX_BATCH_SIZE + 1;
    private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 500000000; //500MB

    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = Duration.ofSeconds(10).toMillis();

    private static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 10000000;
    private AsyncClientProvider provider;

    private RowValueSerializer<A> serializer;

    private RateLimitingStrategy strategy = null;

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
                this.provider,
                this.strategy,
                new ProtoElementConverter<>(this.serializer),
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes()
      );
    }
}
