package com.vinted.flink.bigquery.process;

import com.google.cloud.bigquery.TableId;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.model.Rows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RowBatcher<A extends BigQueryRecord, K, W extends Window> extends ProcessWindowFunction<A, Rows<A>, K, W> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void process(K k, ProcessWindowFunction<A, Rows<A>, K, W>.Context context, Iterable<A> batch, Collector<Rows<A>> out) throws Exception {
        var table = getTable(batch.iterator().next());
        var data = StreamSupport.stream(batch.spliterator(), false).collect(Collectors.toList());
        var result = Rows.defaultStream(data, table);
        out.collect(result);
    }

    private TableId getTable(A data) {
        return data.getTable();
    }
}
