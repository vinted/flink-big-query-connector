package com.vinted.flink.bigquery.metric;

import java.util.concurrent.atomic.AtomicInteger;

public class AsyncBigQueryStreamMetrics {

    private long batchCount = 0;
    private double batchSizeInMb = 0.0;
    private long splitBatchCount = 0;
    private int timeoutCount = 0;
    private final AtomicInteger inflightRequests = new AtomicInteger(0);

    public void incSplitCount() {
        splitBatchCount += 1;
    }
    public void updateSize(long sizeInBytes) {
        batchSizeInMb = sizeInBytes / 1000000.0;
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

    public int getInflightRequests() {
        return inflightRequests.get();
    }

    public void incrementInflightRequests() {
        this.inflightRequests.incrementAndGet();
    }

    public void decrementInflightRequests() {
        this.inflightRequests.decrementAndGet();
    }
}
