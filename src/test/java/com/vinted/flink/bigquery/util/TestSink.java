package com.vinted.flink.bigquery.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.util.ArrayList;
import java.util.List;

public class TestSink<T> implements Sink<T> {
    private static final ArrayList<Object> state = new ArrayList<>();

    public static void clear() {
        synchronized (TestSink.state) {
            state.clear();
        }
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        return new SinkWriter<T>() {
            @Override
            public void write(T element, Context context) {
                synchronized (TestSink.state) {
                    TestSink.state.add(element);
                }
            }

            @Override
            public void flush(boolean endOfInput) {
            }

            @Override
            public void close() {
            }
        };
    }

    @SuppressWarnings("unchecked")
    public List<T> getResults(JobExecutionResult jobResult) {
        return (List<T>) TestSink.state;
    }
}
