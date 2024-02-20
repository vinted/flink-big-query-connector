package com.vinted.flink.bigquery;

import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.connector.sink2.Sink;
import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.serializer.NoOpRowSerializer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import com.vinted.flink.bigquery.sink.ExecutorProvider;
import com.vinted.flink.bigquery.sink.buffered.BigQueryBufferedSink;
import com.vinted.flink.bigquery.sink.defaultStream.BigQueryDefaultSink;
import org.apache.flink.connector.base.DeliveryGuarantee;

public class BigQueryStreamSink<A> {
    private RowValueSerializer<A> rowValueSerializer = new NoOpRowSerializer<>();
    private ClientProvider<A> clientProvider =  null;

    private ExecutorProvider executorProvider = MoreExecutors::directExecutor;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private BigQueryStreamSink() {
    }

    public static <A> BigQueryStreamSink<A> newBuilder() {
        return new BigQueryStreamSink<>();
    }

    public BigQueryStreamSink<A> withRowValueSerializer(RowValueSerializer<A> serializer) {
        this.rowValueSerializer = serializer;
        return this;
    }

    public BigQueryStreamSink<A> withClientProvider(ClientProvider<A> clientProvider) {
        this.clientProvider = clientProvider;
        return this;
    }

    public BigQueryStreamSink<A> withExecutorProvider(ExecutorProvider executorProvider) {
        this.executorProvider = executorProvider;
        return this;
    }

    public BigQueryStreamSink<A> withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public Sink<Rows<A>> build() {
        if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
            return new BigQueryDefaultSink<>(this.rowValueSerializer, this.clientProvider, executorProvider);
        }
        return new BigQueryBufferedSink<>(this.rowValueSerializer, this.clientProvider, executorProvider);
    }
}
