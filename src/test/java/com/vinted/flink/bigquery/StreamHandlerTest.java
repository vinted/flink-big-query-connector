package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.util.MockClock;
import com.vinted.flink.bigquery.util.MockJsonClientProvider;
import com.vinted.flink.bigquery.process.BatchTrigger;
import com.vinted.flink.bigquery.process.StreamStateHandler;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(FlinkTest.class)
public class StreamHandlerTest {
    TableId testTable = TableId.of("test-project", "test-dataset", "test-table");

    @Test
    public void shouldReleaseTwoBatches(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2", "stream3");

        List<Rows<Record>> result = runner.run(pipeline(clientProvider, clock, List.of(new Record("key", "1"), new Record("key", "2"), new Record("key", "3"), new Record("key", "4"))));

        assertThat(result)
                .usingRecursiveComparison()
                .isEqualTo(List.of(
                        new Rows<>(List.of(new Record("key", "1"), new Record("key", "2")), 0, "stream1", testTable),
                        new Rows<>(List.of(new Record("key", "3"), new Record("key", "4")), 2, "stream1", testTable)
                ));
    }

    @Test
    public void shouldTrackOffsetsByKey(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2", "stream3");

        List<Rows<Record>> result = runner.run(pipeline(
                clientProvider, clock, List.of(
                        new Record("key", "1"), new Record("key", "2"),
                        new Record("key2", "3"), new Record("key2", "4"),
                        new Record("key2", "5"), new Record("key2", "6")
                )));

        assertThat(result)
                .usingRecursiveComparison()
                .isEqualTo(List.of(
                        new Rows<>(List.of(new Record("key", "1"), new Record("key", "2")), 0, "stream1", testTable),
                        new Rows<>(List.of(new Record("key2", "3"), new Record("key2", "4")), 0, "stream2", testTable),
                        new Rows<>(List.of(new Record("key2", "5"), new Record("key2", "6")), 2, "stream2", testTable)
                ));
    }

    @Test
    public void shouldCreateStreamWhenReceivingNewBatch(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2", "stream3");

        runner.run(pipeline(clientProvider, clock, List.of(new Record("key", "1"), new Record("key", "2"))));

        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
        verify(clientProvider.getClient(), times(1)).createWriteStream(bqClientCaptor.capture());
    }

    @Test
    public void shouldUseSameStreamForOtherBatchesWithSameKeys(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2", "stream3");

        runner.run(pipeline(clientProvider, clock, List.of(new Record("key", "1"), new Record("key", "2"), new Record("key", "3"), new Record("key", "4"))));

        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
        verify(clientProvider.getClient(), times(1)).createWriteStream(bqClientCaptor.capture());
    }

    @Test
    public void shouldReuseStreamWhenRecoveringFromCheckpointAndStreamNotExpired(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2", "stream3");

        runner
                .withErrorAfter(5)
                .run(pipeline(clientProvider, clock, List.of(
                        new Record("key", "1"), new Record("key", "2"),
                        new Record("key", "3"), new Record("key", "4"),
                        new Record("key", "5"), new Record("key", "6"),
                        new Record("key", "7"), new Record("key", "8"),
                        new Record("key", "9"), new Record("key", "10"),
                        new Record("key", "11"), new Record("key", "12")
                )));

        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);

        verify(clientProvider.getClient(), times(1)).createWriteStream(bqClientCaptor.capture());
        verify(clientProvider.getClient(), times(1)).getWriteStream("stream1");
    }

    @Test
    public void shouldReuseStreamWhenRecoveringFromCheckpointAndStreamIsExpired(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream2", "stream3", "stream4");
        clock.givenCurrentMillis(10, 20, 30, 40, 50, TimeUnit.DAYS.toMillis(20));

        var result = runner
                .withErrorAfter(2)
                .run(pipeline(clientProvider, clock, List.of(
                        new Record("key", "1"), new Record("key", "2"),
                        new Record("key", "3"), new Record("key", "4"),
                        new Record("key", "5"), new Record("key", "6"),
                        new Record("key", "7"), new Record("key", "8"),
                        new Record("key", "9"), new Record("key", "10")
                )));


        var streams = result.stream().collect(Collectors.groupingBy(Rows::getStream));
        assertThat(streams).hasSize(2);


        var stream1Records = streams.get("stream2").stream().map(Rows::getData).collect(Collectors.toList());
        var stream2Records = streams.get("stream3").stream().map(Rows::getData).collect(Collectors.toList());

        assertThat(stream1Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "9"), new Record("key", "10"));

        assertThat(stream2Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "9"), new Record("key", "10"));


        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
        verify(clientProvider.getClient(), times(2)).createWriteStream(bqClientCaptor.capture());
    }

    @Test
    public void shouldRecreateStreamWhenNotFound(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2");
        clientProvider.givenStreamDoesNotExist("stream1");

        var result = runner
                .withErrorAfter(5)
                .run(pipeline(clientProvider, clock, List.of(
                        new Record("key", "1"), new Record("key", "2"),
                        new Record("key", "3"), new Record("key", "4"),
                        new Record("key", "5"), new Record("key", "6"),
                        new Record("key", "7"), new Record("key", "8"),
                        new Record("key", "9"), new Record("key", "10"),
                        new Record("key", "11"), new Record("key", "12")
                )));


        var streams = result.stream().collect(Collectors.groupingBy(Rows::getStream));
        assertThat(streams).hasSize(2);

        var stream1Records = streams.get("stream1").stream().map(Rows::getData).collect(Collectors.toList());
        var stream2Records = streams.get("stream2").stream().map(Rows::getData).collect(Collectors.toList());

        assertThat(stream1Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "11"), new Record("key", "12"));

        assertThat(stream2Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "11"), new Record("key", "12"));


        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
        verify(clientProvider.getClient(), times(2)).createWriteStream(bqClientCaptor.capture());
    }

    @Test
    public void shouldRecreateStreamWhenStreamIsFinalized(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2");
        clientProvider.givenStreamIsFinalized("stream1");

        var result = runner
                .withErrorAfter(5)
                .run(pipeline(clientProvider, clock, List.of(
                        new Record("key", "1"), new Record("key", "2"),
                        new Record("key", "3"), new Record("key", "4"),
                        new Record("key", "5"), new Record("key", "6"),
                        new Record("key", "7"), new Record("key", "8"),
                        new Record("key", "9"), new Record("key", "10"),
                        new Record("key", "11"), new Record("key", "12")
                )));


        var streams = result.stream().collect(Collectors.groupingBy(Rows::getStream));
        assertThat(streams).hasSize(2);

        var stream1Records = streams.get("stream1").stream().map(Rows::getData).collect(Collectors.toList());
        var stream2Records = streams.get("stream2").stream().map(Rows::getData).collect(Collectors.toList());

        assertThat(stream1Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "11"), new Record("key", "12"));

        assertThat(stream2Records)
                .usingRecursiveComparison()
                .asList()
                .doesNotContain(new Record("key", "11"), new Record("key", "12"));

        var bqClientCaptor = ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
        verify(clientProvider.getClient(), times(2)).createWriteStream(bqClientCaptor.capture());
    }

    @Test
    public void shouldFailWhenUnhandledExceptionIsThrown(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider clientProvider, @FlinkTest.FlinkParam MockClock clock) throws Exception {
        clientProvider.givenCreateStream("stream1", "stream2");
        clientProvider.givenGettingStreamFails("stream1");
        assertThatThrownBy(() -> {
            runner
                    .withErrorAfter(3)
                    .run(pipeline(clientProvider, clock, List.of(
                            new Record("key", "1"), new Record("key", "2"),
                            new Record("key", "3"), new Record("key", "4"),
                            new Record("key", "5"), new Record("key", "6"),
                            new Record("key", "7"), new Record("key", "8")
                    )));
        }).isInstanceOf(JobExecutionException.class);
    }

    private Function<StreamExecutionEnvironment, DataStream<Rows<Record>>> pipeline(MockJsonClientProvider provider, MockClock clock, List<Record> elements) {
        return (env) -> {
            Trigger<Record, GlobalWindow> trigger = BatchTrigger.<Record, GlobalWindow>builder()
                    .withCount(2)
                    .withTimeout(Duration.ofMinutes(1))
                    .withResetTimerOnNewRecord(true)
                    .withSizeInMb(1)
                    .build();

            return env.fromCollection(elements)
                    .keyBy(s -> s.key)
                    .window(GlobalWindows.create())
                    .trigger(trigger)
                    .process(new StreamStateHandler<>(provider) {
                        @Override
                        protected Clock getClock() {
                            return clock.get();
                        }
                    }).uid("unique-state");
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
