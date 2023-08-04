package com.vinted.flink.bigquery.sink.buffered;

public class BigQueryCommittable {
    private String streamName;
    private long offset;

    public BigQueryCommittable() {
    }

    public BigQueryCommittable(String streamName, long offset) {
        this.streamName = streamName;
        this.offset = offset;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
