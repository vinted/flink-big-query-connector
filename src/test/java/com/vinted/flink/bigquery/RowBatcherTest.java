package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.TableId;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.util.MockClock;
import com.vinted.flink.bigquery.process.BatchTrigger;
import com.vinted.flink.bigquery.process.RowBatcher;
import com.vinted.flink.bigquery.util.MockJsonClientProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(FlinkTest.class)
public class RowBatcherTest {
    TableId testTable = TableId.of("test-project", "test-dataset", "test-table");

    @Test
    public void shouldReleaseTwoBatches(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        List<Rows<Record>> result = runner.run(pipeline(clientProvider, clock, List.of(new Record("key", "1"), new Record("key", "2"), new Record("key", "3"), new Record("key", "4"))));

        assertThat(result)
                .usingRecursiveComparison()
                .isEqualTo(List.of(
                        new Rows<>(List.of(new Record("key", "1"), new Record("key", "2")), -1, "projects/test-project/datasets/test-dataset/tables/test-table/_default", testTable),
                        new Rows<>(List.of(new Record("key", "3"), new Record("key", "4")), -1, "projects/test-project/datasets/test-dataset/tables/test-table/_default", testTable)
                ));
    }


    private Function<StreamExecutionEnvironment, DataStream<Rows<Record>>> pipeline(MockJsonClientProvider provider, MockClock clock, List<Record> elements) {
        return (env) -> {
            Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                    .withCount(2)
                    .withTimeout(Duration.ofMinutes(1))
                    .withResetTimerOnNewRecord(true)
                    .withSizeInMb(1)
                    .build();

            //var table = new Table(testTable.getProject(), testTable.getDataset(), testTable.getTable());
            return env.fromCollection(elements)
                    .keyBy(s -> s.key)
                    .window(GlobalWindows.create())
                    .trigger(trigger)
                    .process(new RowBatcher<>() {
                    });
        };

    }

    static class Record implements Serializable, BigQueryRecord {
        public String key;
        public String value;

        public Record(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public TableId getTable() {
            return TableId.of("test-project", "test-dataset", "test-table");
        }

        @Override
        public long getSize() {
            return this.value.getBytes().length;
        }
    }
}
