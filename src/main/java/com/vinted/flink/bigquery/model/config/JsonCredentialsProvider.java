package com.vinted.flink.bigquery.model.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;

public class JsonCredentialsProvider implements Credentials {
    private String serviceAccountKey;

    public JsonCredentialsProvider(String serviceAccountKey) {
        this.serviceAccountKey = serviceAccountKey;
    }

    public String getServiceAccountKey() {
        return serviceAccountKey;
    }

    public void setServiceAccountKey(String serviceAccountKey) {
        this.serviceAccountKey = serviceAccountKey;
    }

    @Override
    public CredentialsProvider toProvider() {
        return () -> GoogleCredentials
                .fromStream(new ByteArrayInputStream(serviceAccountKey.getBytes()))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
}
