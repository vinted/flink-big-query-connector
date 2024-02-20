package com.vinted.flink.bigquery.util;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.client.BigQueryJsonClientProvider;
import com.vinted.flink.bigquery.client.BigQueryStreamWriter;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import io.grpc.Status;
import io.grpc.StatusException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class MockJsonClientProvider<A> implements ClientProvider<A>, Serializable {
    private static BigQueryWriteClient mockClient = Mockito.mock(BigQueryWriteClient.class);
    private static JsonStreamWriter writer = Mockito.mock(JsonStreamWriter.class);

    private static AtomicInteger numOfCreatedWriters = new AtomicInteger(0);

    private int retryCount = 5;


    public void givenRetryCount(int count) {
        this.retryCount = count;
    }
    public void givenStreamDoesNotExist(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.NOT_FOUND)))
                .when(MockJsonClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenStreamIsFinalized(String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var exception = createFinalizedStreamException();
        var ex = new RuntimeException(exception);
        Mockito.when(MockJsonClientProvider.mockClient.getWriteStream(streamName))
                .thenThrow(ex);

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(exception));
    }

    public void givenGettingStreamFails(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.INTERNAL)))
                .when(MockJsonClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenCreateStream(String... streamNames) {
        var list = List.of(streamNames);
        var mock = Mockito.when(MockJsonClientProvider.mockClient.createWriteStream(Mockito.nullable(CreateWriteStreamRequest.class)))
                .thenReturn(WriteStream
                        .newBuilder()
                        .setName(list.get(0))
                        .buildPartial()
                );
        list.subList(1, list.size()).forEach(name -> {
            mock.thenReturn(WriteStream
                    .newBuilder()
                    .setName(name)
                    .buildPartial());
        });
    }

    public void givenSuccessfulAppend() throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponse());

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any()))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenFailingAppendWithStatus(Status status) throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(new StatusException(status)));

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any()))
                .thenReturn(createAppendRowsResponseError(new StatusException(status)));
    }

    public void givenTimeoutForAppend() throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createTimeoutAppendRowsResponse());
    }


    public void givenAppendingExistingOffset(int expected, int actual, String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var offsetMock = createOffsetAlreadyExistsException(expected, actual, streamName);

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(offsetMock))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenAppendingTooLargeBatch() throws Descriptors.DescriptorValidationException, IOException {
        var ex = new StatusException(Status.INVALID_ARGUMENT
                .augmentDescription("MessageSize is too large. Max allow: 10000000 Actual: 12040940 status: INVALID_ARGUMENT stream: project"));

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(ex))
                .thenReturn(createAppendRowsResponse());

        Mockito.when(MockJsonClientProvider.writer.append(Mockito.any()))
                .thenReturn(createAppendRowsResponseError(ex))
                .thenReturn(createAppendRowsResponse());
    }

    public int getNumOfCreatedWriter() {
        return numOfCreatedWriters.get();
    }

    public static void reset() {
        Mockito.reset(MockJsonClientProvider.mockClient);
        Mockito.reset(MockJsonClientProvider.writer);
        MockJsonClientProvider.numOfCreatedWriters.set(0);
    }

    private static Exceptions.StreamFinalizedException createFinalizedStreamException() {
        var exception = Mockito.mock(Exceptions.StreamFinalizedException.class);
        Mockito.when(exception.getStatus()).thenReturn(Status.INVALID_ARGUMENT);
        Mockito.when(exception.getCause()).thenReturn(new RuntimeException());
        return exception;
    }

    private static Exceptions.OffsetAlreadyExists createOffsetAlreadyExistsException(long expected, long actual, String streamName) {
        var offsetMock = Mockito.mock(Exceptions.OffsetAlreadyExists.class);
        Mockito.when(offsetMock.getStatus()).thenReturn(Status.ALREADY_EXISTS);
        Mockito.when(offsetMock.getStreamName()).thenReturn(streamName);
        Mockito.when(offsetMock.getExpectedOffset()).thenReturn(expected);
        Mockito.when(offsetMock.getActualOffset()).thenReturn(actual);
        Mockito.when(offsetMock.getCause()).thenReturn(new RuntimeException());
        return offsetMock;
    }

    private static SettableApiFuture<AppendRowsResponse> createAppendRowsResponse() {
        SettableApiFuture<AppendRowsResponse> result = SettableApiFuture.create();
        result.set(AppendRowsResponse.newBuilder().buildPartial());
        return result;
    }

    private static SettableApiFuture<AppendRowsResponse> createTimeoutAppendRowsResponse() {
        SettableApiFuture<AppendRowsResponse> result = SettableApiFuture.create();
        return result;
    }

    private static SettableApiFuture<AppendRowsResponse> createAppendRowsResponseError(Throwable exception) {
        SettableApiFuture<AppendRowsResponse> result = SettableApiFuture.create();
        result.setException(exception);
        return result;
    }

    public JsonStreamWriter getMockJsonWriter() {
        return MockJsonClientProvider.writer;
    }

    @Override
    public BigQueryWriteClient getClient() {
        return MockJsonClientProvider.mockClient;
    }

    @Override
    public BigQueryStreamWriter<A> getWriter(String streamName, TableId table, RowValueSerializer<A> serializer) {
        numOfCreatedWriters.incrementAndGet();
        Mockito.when(MockJsonClientProvider.writer.getWriterId()).thenReturn(UUID.randomUUID().toString());
        return new com.vinted.flink.bigquery.client.JsonStreamWriter<>(serializer, MockJsonClientProvider.writer);
    }


    @Override
    public WriterSettings writeSettings() {
        return WriterSettings.newBuilder().withRetryCount(retryCount).build();
    }
}
