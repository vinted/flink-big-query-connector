package com.vinted.flink.bigquery.serializer;

public class NoOpRowSerializer<A> implements RowValueSerializer<A>{
    @Override
    public byte[] serialize(A value) {
        throw new RuntimeException("Not supported");
    }
}
