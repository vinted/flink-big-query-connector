package com.vinted.flink.bigquery.client;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.vinted.flink.bigquery.model.config.WriterSettings;

import java.io.Serializable;

public interface ClientProvider<A> extends Serializable {
    BigQueryWriteClient getClient();

    A getWriter(String streamName, TableId table);

    WriterSettings writeSettings();
}
