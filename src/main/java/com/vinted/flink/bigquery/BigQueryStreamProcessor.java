package com.vinted.flink.bigquery;

import com.vinted.flink.bigquery.client.ClientProvider;
import com.vinted.flink.bigquery.model.BigQueryRecord;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.process.RowBatcher;
import com.vinted.flink.bigquery.process.StreamStateHandler;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BigQueryStreamProcessor<A extends BigQueryRecord, K, W extends Window> {
    private ClientProvider<?> clientProvider;
    private DeliveryGuarantee deliveryGuarantee;

    public BigQueryStreamProcessor<A, K, W> withClientProvider(ClientProvider<?> clientProvider) {
        this.clientProvider = clientProvider;
        return this;
    }

    public BigQueryStreamProcessor<A, K, W> withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public ProcessWindowFunction<A, Rows<A>, K, W> build() {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            return new StreamStateHandler<>(this.clientProvider);
        }
        return new RowBatcher<>();
    }
}
