package com.vinted.flink.bigquery.model.config;

import com.google.api.gax.core.CredentialsProvider;

import java.io.IOException;
import java.io.Serializable;

public interface Credentials extends Serializable {
    CredentialsProvider toProvider();

    default com.google.auth.Credentials getCredentials() {
        try {
            return this.toProvider().getCredentials();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
