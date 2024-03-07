package com.vinted.flink.bigquery.model.config;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

public class WriterSettings implements Serializable {

    private int streamsPerTable;
    private int writerThreads;
    private Duration timeout;
    private int retryCount;
    private Duration retryPause;
    private Long maxInflightRequests;
    private Long maxInflightBytes;
    private Duration maxRetryDuration;

    private Duration maxRequestWaitCallbackTime;
    private Boolean enableConnectionPool;

    public int getStreamsPerTable() {
        return streamsPerTable;
    }

    public int getWriterThreads() {
        return writerThreads;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Duration getRetryPause() {
        return retryPause;
    }

    public Long getMaxInflightRequests() {
        return maxInflightRequests;
    }

    public Long getMaxInflightBytes() {
        return maxInflightBytes;
    }

    public Duration getMaxRetryDuration() {
        return maxRetryDuration;
    }

    public Boolean getEnableConnectionPool() {
        return enableConnectionPool;
    }

    public BigQueryWriteSettings toBqWriteSettings(Credentials credentials) {
        try {
            return BigQueryWriteSettings
                    .newBuilder()
                    .setCredentialsProvider(credentials.toProvider())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static WriterSettingsBuilder newBuilder() {
        return new WriterSettingsBuilder();
    }

    public Duration getMaxRequestWaitCallbackTime() {
        return maxRequestWaitCallbackTime;
    }

    public void setMaxRequestWaitCallbackTime(Duration maxRequestWaitCallbackTime) {
        this.maxRequestWaitCallbackTime = maxRequestWaitCallbackTime;
    }

    public static final class WriterSettingsBuilder implements Serializable {
        private int streamsPerTable = 1;
        private int writerThreads = 1;
        private Duration timeout = Duration.ofSeconds(10);
        private int retryCount = 5;
        private Duration retryPause = Duration.ofSeconds(5);
        private Long maxInflightRequests = 1000L;
        private Long maxInflightBytes = 100L * 1024L * 1024L; // 100Mb.
        private Duration maxRetryDuration = Duration.ofMinutes(5);
        private Duration maxRequestWaitCallbackTime = Duration.ofMinutes(5);
        private Boolean enableConnectionPool = false;

        private WriterSettingsBuilder() {
        }

        public WriterSettingsBuilder withStreamsPerTable(int streamsPerTable) {
            this.streamsPerTable = streamsPerTable;
            return this;
        }

        public WriterSettingsBuilder withWriterThreads(int writerThreads) {
            this.writerThreads = writerThreads;
            return this;
        }

        public WriterSettingsBuilder withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public WriterSettingsBuilder withRetryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public WriterSettingsBuilder withRetryPause(Duration retryPause) {
            this.retryPause = retryPause;
            return this;
        }

        public WriterSettingsBuilder withMaxInflightRequests(Long maxInflightRequests) {
            this.maxInflightRequests = maxInflightRequests;
            return this;
        }

        public WriterSettingsBuilder withMaxInflightBytes(Long maxInflightBytes) {
            this.maxInflightBytes = maxInflightBytes;
            return this;
        }

        public WriterSettingsBuilder withMaxRetryDuration(Duration maxRetryDuration) {
            this.maxRetryDuration = maxRetryDuration;
            return this;
        }

        public WriterSettingsBuilder withMaxRequestWaitCallbackTime(Duration maxRequestWaitCallbackTime) {
            this.maxRequestWaitCallbackTime = maxRequestWaitCallbackTime;
            return this;
        }

        public WriterSettingsBuilder withEnableConnectionPool(Boolean enableConnectionPool) {
            this.enableConnectionPool = enableConnectionPool;
            return this;
        }

        public WriterSettings build() {
            WriterSettings writerSettings = new WriterSettings();
            writerSettings.writerThreads = this.writerThreads;
            writerSettings.timeout = this.timeout;
            writerSettings.streamsPerTable = this.streamsPerTable;
            writerSettings.retryCount = this.retryCount;
            writerSettings.enableConnectionPool = this.enableConnectionPool;
            writerSettings.maxInflightBytes = this.maxInflightBytes;
            writerSettings.maxInflightRequests = this.maxInflightRequests;
            writerSettings.retryPause = this.retryPause;
            writerSettings.maxRetryDuration = this.maxRetryDuration;
            writerSettings.maxRequestWaitCallbackTime = this.maxRequestWaitCallbackTime;
            return writerSettings;
        }
    }
}


