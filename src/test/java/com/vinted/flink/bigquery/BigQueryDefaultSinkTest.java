package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.protobuf.Int64Value;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.util.MockJsonClientProvider;
import io.grpc.Status;
import com.vinted.flink.bigquery.serializer.JsonRowValueSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;


@ExtendWith(FlinkTest.class)
public class BigQueryDefaultSinkTest {
    TableId testTable = TableId.of("test-project", "test-dataset", "test-table");
    String stream = "projects/test/datasets/test/tables/test/streams/stream1";

    @Test
    public void shouldAppendRows(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenSuccessfulAppend();

        runner.runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                givenRowWithOffset(1, 0)
        ))));

        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), eq(0L));
    }

    @Test
    public void shouldFlushRowsWhenExactlyOnceDeliveryEnabled(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenSuccessfulAppend();

        runner.runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                givenRowWithOffset(1, 1)
        ))));

        verify(mockClientProvider.getClient(), times(1)).flushRows(
                FlushRowsRequest.newBuilder()
                        .setWriteStream(stream)
                        .setOffset(Int64Value.of(1))
                        .build()
        );
    }

    @Test
    public void shouldRetryAppendWhenFailingWithInternalError(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.INTERNAL);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(6)).append(any(), anyLong());
    }

    @Test
    @Disabled("Retry causes out of order exception in committer and later in writer")
    public void shouldRetryOnTimeoutException(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenTimeoutForAppend();

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(6)).append(any(), anyLong());
    }


    @Test
    public void shouldDoNothingWhenFullBatchWasAlreadyAppended(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenAppendingExistingOffset(16, 4, stream);

        runner
                .withRetryCount(0)
                .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                        givenRowWithOffset(4, 4)
                ))));


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), anyLong());
    }

    @Test
    public void shouldSplitBatchWhenAppendingBatchWhereNotAllRowsAreAppended(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenAppendingExistingOffset(4, 2, stream);

        runner
                .withRetryCount(0)
                .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                        givenRowWithOffset(6, 2)
                ))));


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), eq(2L));
        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), eq(4L));
    }

    @Test
    public void shouldFailAndNotRetryWhenFailedWithOutOfRange(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.OUT_OF_RANGE);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 0)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), anyLong());
    }

    @Test
    public void shouldFailAndNotRetryWhenAppendingFailedWithAlreadyExistsWithoutOffsetInformation(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.ALREADY_EXISTS);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 0)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), anyLong());
    }

    @Test
    public void shouldFailAndNotRetryWhenAppendingFailedWithInvalidArgument(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.INVALID_ARGUMENT);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 0)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), anyLong());
    }

    @Test
    public void shouldFailAndNotRetryWhenAppendingToFinalizedStream(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenStreamIsFinalized(stream);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRowWithOffset(1, 0)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), anyLong());
    }

    @Test
    public void shouldSplitTheBatchWhenAppendingTooLargeBatch(@FlinkTest.FlinkParam FlinkTest.PipelineRunner runner, @FlinkTest.FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenAppendingTooLargeBatch();

        runner
                .withRetryCount(0)
                .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                        givenRowWithOffset(6, 4)
                ))));


        verify(mockClientProvider.getMockJsonWriter(), times(2)).append(any(), eq(4L));
        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any(), eq(7L));
    }

    private Rows<String> givenRowWithOffset(int count, int offset) {
        var data = new ArrayList<String>(count);
        IntStream.rangeClosed(1, count)
                .forEach(i -> data.add("{\"value\": " + i + "}"));

        return new Rows<>(data, offset, stream, testTable);
    }

    private Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline(List<Rows<String>> data) {
        return env -> env.fromCollection(data);
    }

    private Function<StreamExecutionEnvironment, DataStreamSink<Rows<String>>> withBigQuerySink(MockJsonClientProvider mockClientProvider, Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline) {
        var sink = BigQueryStreamSink.<String>newJson()
                .withClientProvider(mockClientProvider)
                .withDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .withRowValueSerializer((JsonRowValueSerializer<String>) String::getBytes)
                .build();

        return pipeline.andThen(s -> s.sinkTo(sink));
    }
}
