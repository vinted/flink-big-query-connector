package com.vinted.flink.bigquery.model;


import com.google.cloud.bigquery.TableId;

public interface BigQueryRecord {
    TableId getTable();

    long getSize();
}
