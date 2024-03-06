package com.vinted.flink.bigquery.client;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.ByteString;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;

import java.util.stream.Collectors;

public class ProtoStreamWriter<A> implements BigQueryStreamWriter<A>{
    private final RowValueSerializer<A> rowSerializer;

    private final StreamWriter writer;

    public ProtoStreamWriter(RowValueSerializer<A> rowSerializer, StreamWriter writer) {
        this.rowSerializer = rowSerializer;
        this.writer = writer;
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(Rows<A> data) {
        var prows = ProtoRows
                .newBuilder()
                .addAllSerializedRows(data.getData().stream().map(r -> ByteString.copyFrom(rowSerializer.serialize(r))).collect(Collectors.toList()))
                .build();
        return writer.append(prows);
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(Rows<A> data, long offset) {
        var prows = ProtoRows
                .newBuilder()
                .addAllSerializedRows(data.getData().stream().map(r -> ByteString.copyFrom(rowSerializer.serialize(r))).collect(Collectors.toList()))
                .build();
        return writer.append(prows, offset);
    }

    @Override
    public long getInflightWaitSeconds() {
        return writer.getInflightWaitSeconds();
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
