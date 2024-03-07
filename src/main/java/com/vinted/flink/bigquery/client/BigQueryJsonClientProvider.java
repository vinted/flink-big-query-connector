package com.vinted.flink.bigquery.client;

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors;
import com.vinted.flink.bigquery.model.config.Credentials;
import com.vinted.flink.bigquery.model.config.WriterSettings;
import com.vinted.flink.bigquery.schema.SchemaTransformer;
import com.vinted.flink.bigquery.serializer.RowValueSerializer;
import org.threeten.bp.Duration;

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
            var writerBuilder = JsonStreamWriter
                    .newBuilder(streamName, getTableSchema(table), this.getClient())
                    .setEnableConnectionPool(this.writerSettings.getEnableConnectionPool())
                    .setExecutorProvider(executorProvider);

            if (writerSettings.getRetrySettings() != null) {
                var settings = writerSettings.getRetrySettings();
                var retrySettings =
                        RetrySettings.newBuilder()
                                .setInitialRetryDelay(Duration.ofMillis(settings.getInitialRetryDelay().toMillis()))
                                .setRetryDelayMultiplier(settings.getRetryDelayMultiplier())
                                .setMaxAttempts(settings.getMaxRetryAttempts())
                                .setMaxRetryDelay(Duration.ofMillis(settings.getMaxRetryDelay().toMillis()))
                                .build();

                writerBuilder.setRetrySettings(retrySettings);
            }
            JsonStreamWriter.setMaxRequestCallbackWaitTime(this.writerSettings.getMaxRequestWaitCallbackTime());
            return new com.vinted.flink.bigquery.client.JsonStreamWriter<>(serializer, writerBuilder.build());
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
