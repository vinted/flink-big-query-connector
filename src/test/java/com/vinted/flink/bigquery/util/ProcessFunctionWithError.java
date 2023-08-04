package com.vinted.flink.bigquery.util;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

public class ProcessFunctionWithError<T> extends ProcessFunction<T, T> {

    private int errorAfterRecord;

    public ProcessFunctionWithError(int errorAfterRecord) {
        this.errorAfterRecord = errorAfterRecord;
    }

    private static AtomicInteger counter = new AtomicInteger(1);

    public static void clear() {
        counter.set(0);
    }
    @Override
    public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
        var ct = ProcessFunctionWithError.counter.getAndIncrement();
        if (ct == errorAfterRecord) {
            throw new RuntimeException("error");
        }
        Thread.sleep(10);
        out.collect(value);
    }
}
