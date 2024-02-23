package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.TableId;
import io.grpc.Status;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.JsonRowValueSerializer;
import com.vinted.flink.bigquery.util.FlinkTest;
import com.vinted.flink.bigquery.util.FlinkTest.FlinkParam;
import com.vinted.flink.bigquery.util.FlinkTest.PipelineRunner;
import com.vinted.flink.bigquery.util.MockJsonClientProvider;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;


@ExtendWith(FlinkTest.class)
public class BigQueryBufferedSinkTest {
    TableId testTable = TableId.of("test-project", "test-dataset", "test-table");
    String stream = "projects/test/datasets/test/tables/test/streams/stream1";

    @Test
    public void shouldAppendRows(@FlinkParam PipelineRunner runner, @FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenSuccessfulAppend();

        runner.runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                givenRows(1)
        ))));

        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any());
    }

    @Test
    public void shouldSplitTheBatchWhenAppendingTooLargeBatch(@FlinkParam PipelineRunner runner, @FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenAppendingTooLargeBatch();

        runner
                .withRetryCount(0)
                .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                        givenRows(6)
                ))));


        verify(mockClientProvider.getMockJsonWriter(), times(3)).append(any());
    }

    @Test
    public void shouldRetryOnRecoverableException(@FlinkParam PipelineRunner runner, @FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.FAILED_PRECONDITION);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRows(1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(5)).append(any());
    }

    @Test
    public void shouldNotRetryOnNonRecoverableException(@FlinkParam PipelineRunner runner, @FlinkParam MockJsonClientProvider mockClientProvider) throws Exception {
        mockClientProvider.givenFailingAppendWithStatus(Status.PERMISSION_DENIED);

        assertThatThrownBy(() -> {
            runner
                    .withRetryCount(0)
                    .runWithCustomSink(withBigQuerySink(mockClientProvider, pipeline(List.of(
                            givenRows(1)
                    ))));
        }).isInstanceOf(JobExecutionException.class);


        verify(mockClientProvider.getMockJsonWriter(), times(1)).append(any());
    }

    private Rows<String> givenRows(int count) {
        var data = new ArrayList<String>(count);
        IntStream.rangeClosed(1, count)
                .forEach(i -> data.add("{\"value\": " + i + "}"));

        return Rows.defaultStream(data, testTable);
    }

    private Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline(List<Rows<String>> data) {
        return env -> env.fromCollection(data);
    }

    private Function<StreamExecutionEnvironment, DataStreamSink<Rows<String>>> withBigQuerySink(MockJsonClientProvider<String> mockClientProvider, Function<StreamExecutionEnvironment, DataStream<Rows<String>>> pipeline) {
        var sink = BigQueryStreamSink.<String>newBuilder()
                .withClientProvider(mockClientProvider)
                .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .withRowValueSerializer((JsonRowValueSerializer<String>) String::getBytes)
                .build();

        return pipeline.andThen(s -> s.sinkTo(sink));
    }
}
