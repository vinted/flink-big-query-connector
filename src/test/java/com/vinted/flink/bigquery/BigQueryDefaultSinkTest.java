package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.JsonRowValueSerializer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.util.MockJsonClientProvider;
import io.grpc.Status;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(FlinkTest.class)
public class BigQueryDefaultSinkTest {
    TableId testTable = TableId.of("test-project", "test-dataset", "test-table");
    String stream = "projects/test/datasets/test/tables/test/streams/stream1";

    @Test
    public void shouldAppendRows(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenSuccessfulAppend();

        runner.runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                givenRow(1)
        ))));

        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any());
    }

    @Test
    @Disabled("Retry logic causes locking")
    public void shouldRecreateWriterAndRetryFailingWithMaximumRequestCallbackWaitTimeExceededException(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        var cause = new Exceptions.MaximumRequestCallbackWaitTimeExceededException(Duration.ofMinutes(6), "id", Duration.ofMinutes(5));
        mockClientProvider.givenFailingAppendWithStatus(Status.UNKNOWN.withCause(cause));
        mockClientProvider.givenRetryCount(2);


        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRow(1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(2)).append(any());
        assertThat(mockClientProvider.getNumOfCreatedWriter()).isEqualTo(3);
    }

    @Test
    public void shouldFailAndNotRetryWhenAppendingFailedWithInvalidArgument(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.INVALID_ARGUMENT);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRow(1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any());
    }

    @Test
    @Disabled("Retry logic causes locking")
    public void shouldRecreateWriterAndRetryWhenAppendFailedWithUnavailable(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.UNAVAILABLE);
        mockClientProvider.givenRetryCount(2);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRow(1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);

        verify(mockClientProvider.getMockJsonWriter(), times(2)).append(any());
        assertThat(mockClientProvider.getNumOfCreatedWriter()).isEqualTo(3);
    }

    @Test
    @Disabled("Retry logic causes locking")
    public void shouldSplitTheBatchWhenAppendingTooLargeBatch(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenAppendingTooLargeBatch();

        runner
                .withRetryCount(0)
                .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                        givenRow(6)
                ))));


        verify(mockClientProvider.getMockJsonWriter(), times(3)).append(any());
    }

    private Rows<String> givenRow(int count) {
        var data = new ArrayList<String>(count);
        IntStream.rangeClosed(1, count)
                .forEach(i -> data.add("{\"value\": " + i + "}"));

        return new Rows<>(data, -1, stream, testTable);
    }

    private Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline(List<Rows<String>> data) {
        return env -> env.fromCollection(data);
    }

    private Function<StreamExecutionEnvironment, DataStreamSink<Rows<String>>> withBigQuerySink(MockJsonClientProvider<String> mockClientProvider, Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline) {
        var sink = BigQueryStreamSink.<String>newBuilder()
                .withClientProvider(mockClientProvider)
                .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .withRowValueSerializer((RowValueSerializer<String>) String::getBytes)
                .build();

        return pipeline.andThen(s -> s.sinkTo(sink));
    }
}
