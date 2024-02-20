package com.vinted.flink.bigquery.client;

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.model.config.Credentials;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import com.vinted.flink.bigquery.schema.SchemaTransformer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;

public class BigQueryJsonClientProvider<A> implements ClientProvider<A> {
    private Credentials credentials;
    private WriterSettings writerSettings;

    private transient BigQueryWriteClient bigQueryWriteClient;

    public BigQueryJsonClientProvider(Credentials credentials, WriterSettings writerSettings) {
        this.credentials = credentials;
        this.writerSettings = writerSettings;
    }

    @Override
    public BigQueryWriteClient getClient() {
        if (this.bigQueryWriteClient == null) {
            try {
                bigQueryWriteClient = BigQueryWriteClient
                        .create(this.writerSettings.toBqWriteSettings(credentials));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return bigQueryWriteClient;
    }

    @Override
    public BigQueryStreamWriter<A> getWriter(String streamName, TableId table, RowValueSerializer<A> serializer) {
        try {
            var executorProvider = this.writerSettings.getWriterThreads() > 1 ?
                    FixedExecutorProvider.create(Executors.newScheduledThreadPool(writerSettings.getWriterThreads())) :
                    BigQueryWriteSettings.defaultExecutorProviderBuilder().build();
            var writer = JsonStreamWriter
                    .newBuilder(streamName, getTableSchema(table), this.getClient())
                    .setEnableConnectionPool(this.writerSettings.getEnableConnectionPool())
                    .setExecutorProvider(executorProvider)
                    .build();

            return new com.vinted.flink.bigquery.client.JsonStreamWriter<>(serializer, writer);
        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WriterSettings writeSettings() {
        return this.writerSettings;
    }

    TableSchema getTableSchema(TableId tableId) {
        var table = BigQueryOptions
                .newBuilder()
                .setProjectId(tableId.getProject())
                .setCredentials(credentials.getCredentials())
                .build()
                .getService()
                .getTable(tableId.getDataset(), tableId.getTable());
        var schema = Optional.ofNullable(table)
                .orElseThrow(() -> new IllegalArgumentException("Non existing table: " + tableId))
                .getDefinition()
                .getSchema();
        return SchemaTransformer.convertTableSchema(schema);
    }

}
