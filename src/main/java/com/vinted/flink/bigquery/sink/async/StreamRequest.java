package com.vinted.flink.bigquery.sink.async;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ProtoRows;

import java.io.Serializable;

public class StreamRequest implements Serializable {
    private String stream;
    private TableId table;
    private ProtoRows data;

    private int retries;

    public StreamRequest(String stream, TableId table, ProtoRows data, int retries) {
        this.stream = stream;
        this.table = table;
        this.data = data;
        this.retries = retries;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public TableId getTable() {
        return table;
    }

    public void setTable(TableId table) {
        this.table = table;
    }

    public ProtoRows getData() {
        return data;
    }

    public void setData(ProtoRows data) {
        this.data = data;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
}
