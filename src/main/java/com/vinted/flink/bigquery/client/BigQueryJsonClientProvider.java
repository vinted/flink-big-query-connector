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

import java.io.IOException;
import java.util.concurrent.Executors;

public class BigQueryJsonClientProvider implements ClientProvider<JsonStreamWriter> {
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
    public JsonStreamWriter getWriter(String streamName, TableId table) {
        try {
            var executorProvider = this.writerSettings.getWriterThreads() > 1 ?
                    FixedExecutorProvider.create(Executors.newScheduledThreadPool(writerSettings.getWriterThreads())) :
                    BigQueryWriteSettings.defaultExecutorProviderBuilder().build();
            return JsonStreamWriter
                    .newBuilder(streamName, getTableSchema(table), this.getClient())
                    .setEnableConnectionPool(this.writerSettings.getEnableConnectionPool())
                    .setExecutorProvider(executorProvider)
                    .build();
        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WriterSettings writeSettings() {
        return this.writerSettings;
    }

    TableSchema getTableSchema(TableId table) {
        var schema = BigQueryOptions
                .newBuilder()
                .setProjectId(table.getProject())
                .setCredentials(credentials.getCredentials())
                .build()
                .getService()
                .getTable(table.getDataset(), table.getTable())
                .getDefinition()
                .getSchema();

        return SchemaTransformer.convertTableSchema(schema);
    }

}
