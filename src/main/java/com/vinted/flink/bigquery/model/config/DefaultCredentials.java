package com.vinted.flink.bigquery.model.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;

public class DefaultCredentials implements Credentials {
    @Override
    public CredentialsProvider toProvider() {
        return BigQueryWriteSettings.defaultCredentialsProviderBuilder().build();
    }
}
