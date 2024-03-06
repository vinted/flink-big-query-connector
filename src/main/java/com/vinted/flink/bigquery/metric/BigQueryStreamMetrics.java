package com.vinted.flink.bigquery.metric;

public class BigQueryStreamMetrics {
    private String streamName;

    private long offset = 0;
    private long batchCount = 0;
    private double batchSizeInMb = 0.0;
    private long splitBatchCount = 0;

    private int timeoutCount = 0;

    public BigQueryStreamMetrics(String streamName) {
        this.streamName = streamName;
    }

    public void incSplitCount() {
        splitBatchCount += 1;
    }
    public void updateSize(long sizeInBytes) {
        batchSizeInMb = sizeInBytes / 1000000.0;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    public double getBatchSizeInMb() {
        return batchSizeInMb;
    }

    public long getSplitBatchCount() {
        return splitBatchCount;
    }

    public int getTimeoutCount() {
        return timeoutCount;
    }

    public void incrementTimeoutCount() {
        this.timeoutCount++;
    }
}
