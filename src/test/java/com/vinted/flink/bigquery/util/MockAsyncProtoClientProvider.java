package com.vinted.flink.bigquery.util;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.model.config.Credentials;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import com.vinted.flink.bigquery.sink.async.AsyncClientProvider;
import io.grpc.Status;
import io.grpc.StatusException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class MockAsyncProtoClientProvider extends AsyncClientProvider {
    private static BigQueryWriteClient  mockClient = Mockito.mock(BigQueryWriteClient.class);
    private static StreamWriter protoWriter = Mockito.mock(StreamWriter.class);

    private static AtomicInteger numOfCreatedWriters = new AtomicInteger(0);
    private int retryCount = 5;

    public MockAsyncProtoClientProvider() {
        super(null, null);
    }

    @Override
    public StreamWriter getWriter(String streamName, TableId table) {
        numOfCreatedWriters.incrementAndGet();
        Mockito.when(MockAsyncProtoClientProvider.protoWriter.getWriterId()).thenReturn(UUID.randomUUID().toString());
        return MockAsyncProtoClientProvider.protoWriter;
    }

    @Override
    public BigQueryWriteClient getClient() {
        return MockAsyncProtoClientProvider.mockClient;
    }

    @Override
    public WriterSettings writeSettings() {
        return WriterSettings.newBuilder().withRetryCount(retryCount).build();
    }

    public int getNumOfCreatedWriter() {
        return numOfCreatedWriters.get();
    }

    public void givenRetryCount(int count) {
        this.retryCount = count;
    }

    public void givenStreamDoesNotExist(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.NOT_FOUND)))
                .when(MockAsyncProtoClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenStreamIsFinalized(String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var exception =  createFinalizedStreamException();
        var ex = new RuntimeException(exception);
        Mockito.when(MockAsyncProtoClientProvider.mockClient.getWriteStream(streamName))
                .thenThrow(ex);

        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(exception));
    }

    public void givenGettingStreamFails(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.INTERNAL)))
                .when(MockAsyncProtoClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenCreateStream(String... streamNames) {
        var list = new ArrayList<String>();
        for (String i : streamNames) {
            list.add(i);
        }

        var mock = Mockito.when(MockAsyncProtoClientProvider.mockClient.createWriteStream(Mockito.nullable(CreateWriteStreamRequest.class)))
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
        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenFailingAppendWithStatus(Status status) throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(createAppendRowsResponseError(new StatusException(status)));
    }

    public void givenStreamWriterClosed() throws Descriptors.DescriptorValidationException, IOException {
        var response =  createAppendRowsResponseError(
                new StatusException(Status.ABORTED.withCause(createStreamWriterClosedException()))
        );
        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(response);
    }

    public void givenTimeoutForAppend() throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(createTimeoutAppendRowsResponse());
    }


    public void givenAppendingExistingOffset(int expected, int actual, String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var offsetMock = createOffsetAlreadyExistsException(expected, actual, streamName);

        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(createAppendRowsResponseError(offsetMock))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenAppendingTooLargeBatch() throws Descriptors.DescriptorValidationException, IOException {
        var ex = new StatusException(Status.INVALID_ARGUMENT
                .augmentDescription("MessageSize is too large. Max allow: 10000000 Actual: 12040940 status: INVALID_ARGUMENT stream: project"));

        Mockito.when(MockAsyncProtoClientProvider.protoWriter.append(Mockito.any()))
                .thenReturn(createAppendRowsResponseError(ex))
                .thenReturn(createAppendRowsResponse());
    }

    public static void reset() {
        Mockito.reset(MockAsyncProtoClientProvider.mockClient);
        Mockito.reset(MockAsyncProtoClientProvider.protoWriter);
        MockAsyncProtoClientProvider.numOfCreatedWriters.set(0);
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

    private static Exceptions.StreamWriterClosedException createStreamWriterClosedException() {
        var offsetMock = Mockito.mock(Exceptions.StreamWriterClosedException.class);
        Mockito.when(offsetMock.getStatus()).thenReturn(Status.ABORTED);
        Mockito.when(offsetMock.getStreamName()).thenReturn("stream");
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

    public StreamWriter getMockProtoWriter() {
        return MockAsyncProtoClientProvider.protoWriter;
    }
}
