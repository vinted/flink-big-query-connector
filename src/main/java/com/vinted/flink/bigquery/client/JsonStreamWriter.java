package com.vinted.flink.bigquery.client;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class JsonStreamWriter<A> implements BigQueryStreamWriter<A>{
    private final RowValueSerializer<A> rowSerializer;

    private final com.google.cloud.bigquery.storage.v1.JsonStreamWriter writer;

    public JsonStreamWriter(RowValueSerializer<A> rowSerializer, com.google.cloud.bigquery.storage.v1.JsonStreamWriter writer) {
        this.rowSerializer = rowSerializer;
        this.writer = writer;
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(Rows<A> data) {
        var rowArray = new JSONArray();
        data.getData().forEach(row -> rowArray.put(new JSONObject(new String(rowSerializer.serialize(row)))));
        try {
            return writer.append(rowArray);
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(Rows<A> data, long offset) {
        var rowArray = new JSONArray();
        data.getData().forEach(row -> rowArray.put(new JSONObject(new String(rowSerializer.serialize(row)))));
        try {
            return writer.append(rowArray, offset);
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getStreamName() {
        return writer.getStreamName();
    }

    @Override
    public String getWriterId() {
        return writer.getWriterId();
    }

    @Override
    public boolean isClosed() {
        return writer.isClosed() || writer.isUserClosed();
    }

    @Override
    public void close() throws Exception {
       writer.close();
    }
}
