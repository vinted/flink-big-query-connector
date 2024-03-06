package com.vinted.flink.bigquery.sink.async;

import com.vinted.flink.bigquery.model.Rows;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class AsyncBigQuerySink<A> extends AsyncSinkBase<Rows<A>, StreamRequest> {
    private final AsyncClientProvider provider;
    private final RateLimitingStrategy strategy;

    public static <A> AsyncBigQuerySinkBuilder<A> builder() {
        return new AsyncBigQuerySinkBuilder<>();
    }

    protected AsyncBigQuerySink(AsyncClientProvider provider, RateLimitingStrategy rateLimitingStrategy, ElementConverter<Rows<A>, StreamRequest> elementConverter, int maxBatchSize, int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS, long maxRecordSizeInBytes) {
        super(elementConverter, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes);
        this.provider = provider;
        this.strategy = rateLimitingStrategy;
    }

    @Override
    public StatefulSinkWriter<Rows<A>, BufferedRequestState<StreamRequest>> createWriter(InitContext initContext) throws IOException {
        return new AsyncBigQuerySinkWriter<>(provider, this.getElementConverter(), initContext,
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
        return new AsyncBigQuerySinkWriter<>(provider, this.getElementConverter(), initContext,
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
}
