package com.vinted.flink.bigquery.serializer;

import java.io.Serializable;

public interface RowValueSerializer<A> extends Serializable {
    byte[] serialize(A value);
}
