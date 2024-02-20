package com.vinted.flink.bigquery.client;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.vinted.flink.bigquery.model.Rows;

public interface BigQueryStreamWriter<T> extends AutoCloseable {
    ApiFuture<AppendRowsResponse> append(Rows<T> data);
    ApiFuture<AppendRowsResponse> append(Rows<T> data, long offset);

    String getStreamName();

    String getWriterId();
    boolean isClosed();
}
