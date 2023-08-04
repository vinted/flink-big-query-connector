package com.vinted.flink.bigquery.model;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;

import java.io.Serializable;
import java.util.List;

public class Rows<A> implements Serializable {
    private List<A> data;
    private long offset;
    private String stream;
    private TableId table;

    public Rows<A> updateBatch(List<A> data, long offset) {
        return new Rows<>(data, offset, stream, table);
    }

    public static <A> Rows<A> defaultStream(List<A> data, TableId table) {
        var fullPath = TableName.of(table.getProject(), table.getDataset(), table.getTable()).toString();
        return new Rows<>(data, -1, String.format("%s/_default", fullPath), table);
    }

    public Rows() {
    }

    public Rows(List<A> data, long offset, String stream, TableId table) {
        this.data = data;
        this.offset = offset;
        this.stream = stream;
        this.table = table;
    }

    public List<A> getData() {
        return data;
    }

    public void setData(List<A> data) {
        this.data = data;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
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

    @Override
    public String toString() {
        return "Rows{" +
                "dataCount=" + data.size() +
                ", offset=" + offset +
                ", stream='" + stream + '\'' +
                ", table=" + table +
                '}';
    }
}
