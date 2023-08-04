package com.vinted.flink.bigquery.sink.buffered;

import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.JsonRowValueSerializer;
import com.vinted.flink.bigquery.serializer.ProtoValueSerializer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class BigQueryBufferedSink<A, StreamT> implements TwoPhaseCommittingSink<Rows<A>, BigQueryCommittable> {
    private final RowValueSerializer<A> rowValueSerializer;
    private final ClientProvider<StreamT> clientProvider;

    private final ExecutorProvider executorProvider;

    public BigQueryBufferedSink(RowValueSerializer<A> rowValueSerializer, ClientProvider<StreamT> clientProvider, ExecutorProvider executorProvider) {
        this.rowValueSerializer = rowValueSerializer;
        this.clientProvider = clientProvider;
        this.executorProvider = executorProvider;
    }

    @Override
    public PrecommittingSinkWriter<Rows<A>, BigQueryCommittable> createWriter(InitContext context) throws IOException {
        if (rowValueSerializer instanceof JsonRowValueSerializer) {
            return new BigQueryJsonBufferedSinkWriter<>(context, rowValueSerializer, (ClientProvider<JsonStreamWriter>) clientProvider, executorProvider);
        } else if (rowValueSerializer instanceof ProtoValueSerializer) {
            return new BigQueryProtoBufferedSinkWriter<>(context, rowValueSerializer, (ClientProvider<StreamWriter>) clientProvider, executorProvider);
        } else {
            throw new RuntimeException("Not supported serializer");
        }
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter() throws IOException {
        return new BigQuerySinkCommitter(clientProvider);
    }

    @Override
    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        return new BigQueryCommittableSerializer();
    }

}
