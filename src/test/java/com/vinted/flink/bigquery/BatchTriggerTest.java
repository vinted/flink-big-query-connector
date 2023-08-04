package com.vinted.flink.bigquery;


import com.google.cloud.bigquery.TableId;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.process.BatchTrigger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(FlinkTest.class)
public class BatchTriggerTest {
    @Test
    public void shouldTriggerWindowWhenCountIsReached(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner) throws Exception {
        Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                .withCount(2)
                .withTimeout(Duration.ofMinutes(1))
                .withResetTimerOnNewRecord(true)
                .withSizeInMb(1)
                .build();

        List<List<Record>> result = runner.run(pipeline(trigger, List.of("1", "2", "3", "4", "5")));

        assertThat(result).hasSize(2);
        assertThat(result.get(0).stream().map(s -> s.value).collect(Collectors.toList())).containsOnly("1", "2");
        assertThat(result.get(1).stream().map(s -> s.value).collect(Collectors.toList())).containsOnly("3", "4");
    }

    @Test
    public void shouldNotReleaseDataBeforeTimeout(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner) throws Exception {
        Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                .withCount(3)
                .withTimeout(Duration.ofMinutes(1))
                .withResetTimerOnNewRecord(true)
                .withSizeInMb(1)
                .build();

        List<List<Record>> result = runner.run(pipeline(trigger, List.of("1", "2")));

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldReleaseDataAfterTimeout(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner) throws Exception {
        Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                .withCount(3)
                .withTimeout(Duration.ofSeconds(1))
                .withResetTimerOnNewRecord(true)
                .withSizeInMb(1)
                .build();

        List<List<Record>> result = runner.run(pipeline(trigger, List.of("1", "2", "await")));


        assertThat(result).hasSize(1);
        assertThat(result.get(0).stream().map(s -> s.value).collect(Collectors.toList())).containsOnly("1", "2");
    }

    @Test
    public void shouldReleaseDataWhenBatchHitsSizeLimit(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner) throws Exception {
        Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                .withCount(30)
                .withTimeout(Duration.ofSeconds(1))
                .withResetTimerOnNewRecord(true)
                .withSizeInMb(1)
                .build();

        var string1 = givenRandomStringWithSize(400000, "a");
        var string2 = givenRandomStringWithSize(400000, "b");
        var string3 = givenRandomStringWithSize(400000, "c");

        List<List<Record>> result = runner.run(pipeline(trigger, List.of(string1, string2, string3, "end")));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).stream().map(s -> s.value).collect(Collectors.toList())).containsOnly(string1, string2, string3);
    }

    private Function<StreamExecutionEnvironment, DataStream<List<Record>>> pipeline(Trigger<Record, GlobalWindow> trigger, List<String> elements) {
        return (env) ->
                env.fromCollection(new IteratorWithWait(elements.stream().map(Record::new).collect(Collectors.toList())), Record.class)
                        .windowAll(GlobalWindows.create())
                        .trigger(trigger)
                        .process(new Batching())
                        .uid("batching");

    }

    private String givenRandomStringWithSize(int size, String value) {
        var builder = new StringBuilder(size);
        IntStream.rangeClosed(0, size).forEach(a -> {
            builder.append(value);
        });

        return builder.toString();
    }

    static class IteratorWithWait implements Iterator<Record>, Serializable {
        private ArrayDeque<Record> data;

        public IteratorWithWait(List<Record> data) {
            this.data = new ArrayDeque<>(data);
        }

        @Override
        public boolean hasNext() {
            return !data.isEmpty();
        }

        @Override
        public Record next() {
            var i = data.pop();
            if (i.value.equals("await")) {
                try {
                    Thread.sleep(Duration.ofSeconds(2).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return i;
        }
    }

    static class Batching extends ProcessAllWindowFunction<Record, List<Record>, GlobalWindow> {
        @Override
        public void process(ProcessAllWindowFunction<Record, List<Record>, GlobalWindow>.Context context, Iterable<Record> elements, Collector<List<Record>> out) throws Exception {
            var list = new ArrayList<Record>();
            elements.forEach(list::add);
            out.collect(list);
        }

    }

    public static class Record implements Serializable, BigQueryRecord {

        private String value;

        public Record(String value) {
            this.value = value;
        }

        @Override
        public TableId getTable() {
            return TableId.of("test-project", "test-dataset", "test-table");
        }

        @Override
        public long getSize() {
            return value.getBytes().length;
        }
    }
}
