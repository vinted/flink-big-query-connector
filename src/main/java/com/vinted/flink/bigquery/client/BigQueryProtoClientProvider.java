package com.vinted.flink.bigquery.client;

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.model.config.Credentials;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import com.vinted.flink.bigquery.schema.SchemaTransformer;

import java.io.IOException;
import java.util.concurrent.Executors;

public class BigQueryProtoClientProvider implements ClientProvider<StreamWriter> {
    private Credentials credentials;
    private WriterSettings writerSettings;

    private transient BigQueryWriteClient bigQueryWriteClient;

    public BigQueryProtoClientProvider(Credentials credentials, WriterSettings writerSettings) {
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
    public StreamWriter getWriter(String streamName, TableId table) {
        try {
            var descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(getTableSchema(table));
            var protoSchema = ProtoSchemaConverter.convert(descriptor);
            var executorProvider = this.writerSettings.getWriterThreads() > 1 ?
                    FixedExecutorProvider.create(Executors.newScheduledThreadPool(writerSettings.getWriterThreads())) :
                    BigQueryWriteSettings.defaultExecutorProviderBuilder().build();
            var streamWriterBuilder = StreamWriter
                    .newBuilder(streamName, getClient())
                    .setMaxInflightRequests(this.writerSettings.getMaxInflightRequests())
                    .setMaxInflightBytes(this.writerSettings.getMaxInflightBytes())
                    .setMaxRetryDuration(this.writerSettings.getMaxRetryDuration())
                    .setEnableConnectionPool(this.writerSettings.getEnableConnectionPool())
                    .setExecutorProvider(executorProvider)
                    .setLocation(table.getProject())
                    .setWriterSchema(protoSchema);
            return streamWriterBuilder.build();
        } catch (IOException | Descriptors.DescriptorValidationException e) {
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
