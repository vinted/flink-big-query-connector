package com.vinted.flink.bigquery.sink;

import com.vinted.flink.bigquery.model.Rows;

public class AppendException extends RuntimeException {
    private final Rows<?> rows;

    private final String traceId;

    private final Throwable error;

    private final int retryCount;

    public AppendException(String traceId, Rows<?> rows, int retryCount, Throwable error) {
        this.traceId = traceId;
        this.rows = rows;
        this.retryCount = retryCount;
        this.error = error;
    }

    public String getTraceId() {
        return traceId;
    }

    public <A> Rows<A> getRows() {
        return (Rows<A>) rows;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Throwable getError() {
        return error;
    }
}
