package com.vinted.flink.bigquery.sink.buffered;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
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

import java.io.IOException;

public class BigQueryJsonBufferedSinkWriter<A> extends BigQueryBufferedSinkWriter<A, JsonStreamWriter> {
    public BigQueryJsonBufferedSinkWriter(Sink.InitContext sinkInitContext, RowValueSerializer<A> rowSerializer, ClientProvider<JsonStreamWriter> clientProvider, ExecutorProvider executorProvider) {
        super(sinkInitContext, rowSerializer, clientProvider, executorProvider);
    }

    @Override
    protected ApiFuture<AppendRowsResponse> append(String traceId, Rows<A> rows) {
        var rowArray = new JSONArray();
        rows.getData().forEach(row -> rowArray.put(new JSONObject(new String(rowSerializer.serialize(row)))));

        try {
            return streamWriter(traceId, rows.getStream(), rows.getTable()).append(rowArray, rows.getOffset());
        } catch (Throwable t) {
            return ApiFutures.immediateFailedFuture(t);
        }
    }
}
