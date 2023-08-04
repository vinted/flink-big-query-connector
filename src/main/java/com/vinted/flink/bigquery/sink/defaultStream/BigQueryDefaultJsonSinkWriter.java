package com.vinted.flink.bigquery.sink.defaultStream;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.connector.sink2.Sink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BigQueryDefaultJsonSinkWriter<A> extends BigQueryDefaultSinkWriter<A, JsonStreamWriter> {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryDefaultJsonSinkWriter.class);
    public BigQueryDefaultJsonSinkWriter(Sink.InitContext sinkInitContext, RowValueSerializer<A> rowSerializer, ClientProvider<JsonStreamWriter> clientProvider, ExecutorProvider executorProvider) {
        super(sinkInitContext, rowSerializer, clientProvider, executorProvider);
    }

    @Override
    protected ApiFuture<AppendRowsResponse> append(String traceId, Rows<A> rows) {
        var rowArray = new JSONArray();
        rows.getData().forEach(row -> rowArray.put(new JSONObject(new String(rowSerializer.serialize(row)))));
        var writer = streamWriter(traceId, rows.getStream(), rows.getTable());

        if (writer.isClosed() || writer.isUserClosed()) {
            logger.warn("Trace-id {}, StreamWrite is closed. Recreating stream for {}", traceId, rows.getStream());
        }

        logger.trace("Trace-id {}, Writing rows stream {} to steamWriter for {} writer id {}", traceId, rows.getStream(), writer.getStreamName(), writer.getWriterId());

        try {
            return writer.append(rowArray);
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            logger.error("Trace-id {}, StreamWriter failed to append {}", traceId, e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
