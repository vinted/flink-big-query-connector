package com.vinted.flink.bigquery.util;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import io.grpc.Status;
import io.grpc.StatusException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

public class MockProtoClientProvider implements ClientProvider<StreamWriter>, Serializable {
    private static BigQueryWriteClient  mockClient = Mockito.mock(BigQueryWriteClient.class);
    private static StreamWriter protoWriter = Mockito.mock(StreamWriter.class);

    private static void givenExistingStream(String streamName) {
        var stream = WriteStream
                .newBuilder()
                .setName(streamName)
                .build();

        Mockito.doReturn(stream).when(MockProtoClientProvider.mockClient).getWriteStream(Mockito.anyString());
    }

    public void givenStreamDoesNotExist(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.NOT_FOUND)))
                .when(MockProtoClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenStreamIsFinalized(String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var exception =  createFinalizedStreamException();
        var ex = new RuntimeException(exception);
        Mockito.when(MockProtoClientProvider.mockClient.getWriteStream(streamName))
                .thenThrow(ex);

        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(exception));
    }

    public void givenGettingStreamFails(String streamName) {
        Mockito.doThrow(new RuntimeException(new StatusException(Status.INTERNAL)))
                .when(MockProtoClientProvider.mockClient).getWriteStream(streamName);
    }

    public void givenCreateStream(String... streamNames) {
        var list = new ArrayList<String>();
        for (String i : streamNames) {
            list.add(i);
        }

        var mock = Mockito.when(MockProtoClientProvider.mockClient.createWriteStream(Mockito.nullable(CreateWriteStreamRequest.class)))
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
        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenFailingAppendWithStatus(Status status) throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(new StatusException(status)));
    }

    public void givenTimeoutForAppend() throws Descriptors.DescriptorValidationException, IOException {
        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createTimeoutAppendRowsResponse());
    }


    public void givenAppendingExistingOffset(int expected, int actual, String streamName) throws Descriptors.DescriptorValidationException, IOException {
        var offsetMock = createOffsetAlreadyExistsException(expected, actual, streamName);

        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(offsetMock))
                .thenReturn(createAppendRowsResponse());
    }

    public void givenAppendingTooLargeBatch() throws Descriptors.DescriptorValidationException, IOException {
        var ex = new StatusException(Status.INVALID_ARGUMENT
                .augmentDescription("MessageSize is too large. Max allow: 10000000 Actual: 12040940 status: INVALID_ARGUMENT stream: project"));

        Mockito.when(MockProtoClientProvider.protoWriter.append(Mockito.any(), Mockito.anyLong()))
                .thenReturn(createAppendRowsResponseError(ex))
                .thenReturn(createAppendRowsResponse());
    }

    public static void reset() {
        Mockito.reset(MockProtoClientProvider.mockClient);
        Mockito.reset(MockProtoClientProvider.protoWriter);
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

    public StreamWriter getMockProtoWriter() {
        return MockProtoClientProvider.protoWriter;
    }

    @Override
    public BigQueryWriteClient getClient() {
        return MockProtoClientProvider.mockClient;
    }

    @Override
    public StreamWriter getWriter(String streamName, TableId table) {
        return MockProtoClientProvider.protoWriter;
    }


    @Override
    public WriterSettings writeSettings() {
        return WriterSettings.newBuilder().build();
    }
}
