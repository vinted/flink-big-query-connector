package com.vinted.flink.bigquery.sink;

import com.vinted.flink.bigquery.model.Rows;

public class AppendException extends RuntimeException {
    private final Rows<?> rows;

    private final String traceId;

    private final Throwable error;

    public AppendException(String traceId, Rows<?> rows, Throwable error) {
        this.traceId = traceId;
        this.rows = rows;
        this.error = error;
    }

    public String getTraceId() {
        return traceId;
    }

    public <A> Rows<A> getRows() {
        return (Rows<A>) rows;
    }

    public Throwable getError() {
        return error;
    }
}
