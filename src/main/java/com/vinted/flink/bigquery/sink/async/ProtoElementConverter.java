package com.vinted.flink.bigquery.sink.async;

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.util.stream.Collectors;

public class ProtoElementConverter<A> implements ElementConverter<Rows<A>, StreamRequest> {
    private final RowValueSerializer<A> serializer;

    public ProtoElementConverter(RowValueSerializer<A> serializer) {
        this.serializer = serializer;
    }

    @Override
    public StreamRequest apply(Rows<A> rows, SinkWriter.Context context) {
        var prows = ProtoRows
                .newBuilder()
                .addAllSerializedRows(rows.getData().stream().map(r -> ByteString.copyFrom(serializer.serialize(r))).collect(Collectors.toList()))
                .build();
        return new StreamRequest(rows.getStream(), rows.getTable(), prows);
    }
}
