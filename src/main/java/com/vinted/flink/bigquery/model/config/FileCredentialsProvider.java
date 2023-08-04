package com.vinted.flink.bigquery.model.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.BufferedInputStream;
import java.io.FileInputStream;

public class FileCredentialsProvider implements Credentials {
    private String path;

    public FileCredentialsProvider(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public CredentialsProvider toProvider() {
        return () -> GoogleCredentials
                .fromStream(new BufferedInputStream(new FileInputStream(path)))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
}
