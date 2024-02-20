package com.vinted.flink.bigquery.sink.defaultStream;

import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.vinted.flink.bigquery.client.ClientProvider;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.JsonRowValueSerializer;
import com.vinted.flink.bigquery.serializer.ProtoValueSerializer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;

import java.io.IOException;

public class BigQueryDefaultSink<A> implements Sink<Rows<A>> {
    private final RowValueSerializer<A> rowValueSerializer;
    private final ClientProvider<A> clientProvider;
    private final ExecutorProvider executorProvider;

    public BigQueryDefaultSink(
            RowValueSerializer<A> rowValueSerializer,
            ClientProvider<A> clientProvider,
            ExecutorProvider executorProvider) {
        this.rowValueSerializer = rowValueSerializer;
        this.clientProvider = clientProvider;
        this.executorProvider = executorProvider;
    }

    @Override
    public SinkWriter<Rows<A>> createWriter(InitContext context) throws IOException {
        return new BigQueryDefaultSinkWriter<A>(context, rowValueSerializer, clientProvider, executorProvider);
    }

}
