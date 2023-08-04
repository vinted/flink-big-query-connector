package com.vinted.flink.bigquery.process;

import java.util.concurrent.TimeUnit;

public class StreamState {
    private String name;
    private long offset;
    private long lastUpdate;

    public StreamState() {
    }

    public StreamState(String name, long offset, long lastUpdate) {
        this.name = name;
        this.offset = offset;
        this.lastUpdate = lastUpdate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public boolean expired(int ttlDays, long currentTime) {
        return currentTime >= lastUpdate + TimeUnit.DAYS.toMillis(ttlDays);
    }

    public StreamState update(long batchSize, long lastUpdate) {
        return new StreamState(name, offset + batchSize, lastUpdate);
    }

    @Override
    public String toString() {
        return "StreamState{" +
                "name='" + name + '\'' +
                ", offset=" + offset +
                ", lastUpdate=" + lastUpdate +
                '}';
    }
}
