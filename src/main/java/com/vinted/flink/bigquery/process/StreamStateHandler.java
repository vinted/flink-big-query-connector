package com.vinted.flink.bigquery.process;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.model.Rows;
import io.grpc.Status;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StreamStateHandler<A extends BigQueryRecord, K, W extends Window> extends ProcessWindowFunction<A, Rows<A>, K, W> {
    private static final Logger logger = LoggerFactory.getLogger(StreamStateHandler.class);
    private final ClientProvider<?> clientProvider;
    private transient Clock clock;
    private transient HashSet<String> localStreamState;
    private transient BigQueryWriteClient client;
    private transient ValueState<StreamState> streamState;
    private final int streamTTLDays = 7;


    public StreamStateHandler(ClientProvider<?> clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.streamState = this.getRuntimeContext().getState(new ValueStateDescriptor<>(
                "streamState",
                TypeInformation.of(StreamState.class)
        ));
        client = clientProvider.getClient();
        localStreamState = new HashSet<>();
        clock = Clock.systemUTC();
    }

    @Override
    public void process(K k, ProcessWindowFunction<A, Rows<A>, K, W>.Context context, Iterable<A> batch, Collector<Rows<A>> out) throws Exception {
        var table = getTable(batch.iterator().next());
        var value = streamState.value();

        var state = getState(table, value);
        var data = StreamSupport.stream(batch.spliterator(), false).collect(Collectors.toList());
        var result = new Rows<>(data, state.getOffset(), state.getName(), table);
        out.collect(result);

        updateState(state.update(data.size(), getClock().millis()));
    }

    private StreamState getState(TableId table, StreamState value) {
        switch (resolveStatus(value)) {
            case NOT_EXISTS: {
                logger.info("Stream for {} never existed. Creating new stream", table);
                return createStream(table, this::updateState);
            }
            case NOT_CACHED: {
                logger.info("Stream for {} is not in local cache. Getting stream from BigQuery", table);
                try {
                    client.getWriteStream(value.getName());
                    return updateState(value);
                } catch (Throwable error) {
                    switch (Status.fromThrowable(error).getCode()) {
                        case INVALID_ARGUMENT: {
                            if (error.getCause() instanceof Exceptions.StreamFinalizedException) {
                                logger.warn("Stream for {} is finished. Creating new stream", table, error);
                                return createStream(table, this::updateState);
                            }
                            throw error;
                        }
                        case NOT_FOUND: {
                            logger.warn("Stream for {} not found. Creating new stream", table, error);
                            return createStream(table, this::updateState);
                        }
                        default:
                            throw error;
                    }

                }
            }
            case EXPIRED: {
                logger.warn("Stream for {} is old and might be finished due to TTL. Creating new stream", table);
                return createStream(table, this::updateState);
            }
            default:
                return value;
        }
    }


    private StreamState updateState(StreamState newState) {
        localStreamState.add(newState.getName());

        try {
            streamState.update(newState);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return newState;
    }

    private StreamState createStream(TableId table, Function<StreamState, StreamState> stateUpdateFunc) {
        var fullPath = TableName.of(table.getProject(), table.getDataset(), table.getTable()).toString();
        logger.info("Creating new stream for: {}", table);
        var createWriteStreamRequest = CreateWriteStreamRequest
                .newBuilder()
                .setParent(fullPath)
                .setWriteStream(WriteStream
                        .newBuilder()
                        .setType(WriteStream.Type.BUFFERED)
                        .build()
                ).build();
        var stream = client.createWriteStream(createWriteStreamRequest);
        return stateUpdateFunc.apply(new StreamState(stream.getName(), 0L, getClock().millis()));
    }

    private StreamStateStatus resolveStatus(StreamState state) {
        if (state == null) {
            return StreamStateStatus.NOT_EXISTS;
        }

        if (state.expired(streamTTLDays, getClock().millis())) {
            return StreamStateStatus.EXPIRED;
        }

        if (!localStreamState.contains(state.getName())) {
            return StreamStateStatus.NOT_CACHED;
        }

        return StreamStateStatus.CACHED;
    }

    private TableId getTable(A data) {
        return data.getTable();
    }

    protected Clock getClock() {
        return clock;
    }

    enum StreamStateStatus {NOT_CACHED, EXPIRED, CACHED, NOT_EXISTS}
}
