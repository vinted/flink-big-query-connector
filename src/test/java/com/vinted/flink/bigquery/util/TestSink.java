package com.vinted.flink.bigquery.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

public class TestSink<T> extends RichSinkFunction<T> {
    private static ArrayList<Object> state = new ArrayList<>();

    public static void clear() {
        state.clear();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        TestSink.state.add(value);
    }

    public List<T> getResults(JobExecutionResult jobResult) {
        return (List<T>) TestSink.state;
    };
}
