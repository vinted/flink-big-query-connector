package com.vinted.flink.bigquery.sink.async;

import io.grpc.Status;

public class AsyncWriterException extends RuntimeException {

    public AsyncWriterException(String traceId, Status.Code code, Throwable cause) {
        super(
                String.format("Trace-id %s Received error %s with status %s", traceId, cause.getMessage(),
                        code.toString()),
                cause
        );
    }
}
