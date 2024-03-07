package com.vinted.flink.bigquery.model.config;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

public class WriterRetrySettings implements Serializable {

    private Duration initialRetryDelay;
    private double retryDelayMultiplier;

    private int maxRetryAttempts;

    private Duration maxRetryDelay;

    public Duration getInitialRetryDelay() {
        return initialRetryDelay;
    }

    public void setInitialRetryDelay(Duration initialRetryDelay) {
        this.initialRetryDelay = initialRetryDelay;
    }

    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    public void setRetryDelayMultiplier(double retryDelayMultiplier) {
        this.retryDelayMultiplier = retryDelayMultiplier;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public Duration getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public void setMaxRetryDelay(Duration maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
    }
    public static WriterRetrySettingsBuilder newBuilder() {
        return new WriterRetrySettingsBuilder();
    }

    public static final class WriterRetrySettingsBuilder implements Serializable {
        private Duration initialRetryDelay = Duration.ofMillis(500);
        private double retryDelayMultiplier = 1.1;

        private int maxRetryAttempts = 5;

        private Duration maxRetryDelay = Duration.ofMinutes(1);
        private WriterRetrySettingsBuilder() {
        }

        public WriterRetrySettingsBuilder withInitialRetryDelay(Duration initialRetryDelay) {
            this.initialRetryDelay = initialRetryDelay;
            return this;
        }

        public WriterRetrySettingsBuilder withRetryDelayMultiplier(double retryDelayMultiplier) {
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        public WriterRetrySettingsBuilder withMaxRetryAttempts(int maxRetryAttempts) {
            this.maxRetryAttempts = maxRetryAttempts;
            return this;
        }

        public WriterRetrySettingsBuilder withMaxRetryDelay(Duration maxRetryDelay) {
            this.maxRetryDelay = maxRetryDelay;
            return this;
        }

        public WriterRetrySettings build() {
            WriterRetrySettings retrySettings = new WriterRetrySettings();
            retrySettings.initialRetryDelay = this.initialRetryDelay;
            retrySettings.retryDelayMultiplier = this.retryDelayMultiplier;
            retrySettings.maxRetryAttempts = this.maxRetryAttempts;
            retrySettings.maxRetryDelay = this.maxRetryDelay;
            return retrySettings;
        }
    }
}


