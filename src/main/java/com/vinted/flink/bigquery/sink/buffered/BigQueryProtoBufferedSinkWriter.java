package com.vinted.flink.bigquery.sink.buffered;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.ByteString;
import org.apache.flink.api.connector.sink2.Sink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

public class BigQueryProtoBufferedSinkWriter<A> extends BigQueryBufferedSinkWriter<A, StreamWriter> {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryProtoBufferedSinkWriter.class);
    public BigQueryProtoBufferedSinkWriter(Sink.InitContext sinkInitContext, RowValueSerializer<A> rowSerializer, ClientProvider<StreamWriter> clientProvider, ExecutorProvider executorProvider) {
        super(sinkInitContext, rowSerializer, clientProvider, executorProvider);
    }

    @Override
    protected ApiFuture<AppendRowsResponse> append(String traceId, Rows<A> rows) {
        var prows = ProtoRows
                .newBuilder()
                .addAllSerializedRows(rows.getData().stream().map(r -> ByteString.copyFrom(rowSerializer.serialize(r))).collect(Collectors.toList()))
                .build();
        var size = prows.getSerializedSize();
        numBytesOutCounter.inc(size);
        Optional.ofNullable(metrics.get(rows.getStream())).ifPresent(s -> s.updateSize(size));
        var writer = streamWriter(traceId, rows.getStream(), rows.getTable());

        if (writer.isClosed() || writer.isUserClosed()) {
            logger.warn("Trace-id {}, StreamWrite is closed. Recreating stream for {}", traceId, rows.getStream());
        }

        logger.trace("Trace-id {}, Writing rows stream {} to steamWriter for {} writer id {}", traceId, rows.getStream(), writer.getStreamName(), writer.getWriterId());

        try {
            return writer.append(prows, rows.getOffset());
        } catch (Throwable t) {
            logger.error("Trace-id {}, StreamWriter failed to append {}", traceId, t.getMessage());
            return ApiFutures.immediateFailedFuture(t);
        }
    }
}
