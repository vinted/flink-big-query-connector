package com.vinted.flink.bigquery.process;

import com.vinted.flink.bigquery.model.BigQueryRecord;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class BatchTrigger<A extends BigQueryRecord, W extends Window> extends Trigger<A, W> {
    private static final Logger logger = LoggerFactory.getLogger(BatchTrigger.class);
    private final ReducingStateDescriptor<Long> sizeStateDesc =
            new ReducingStateDescriptor<>("size", new BatchTrigger.Sum(), LongSerializer.INSTANCE);
    private CountTrigger<W> countTrigger;
    private int maxSizeMb;

    public static <A extends BigQueryRecord, W extends Window> Builder<A, W> builder() {
        return new Builder<>();
    }

    public BatchTrigger(long maxCount, int maxSizeMb) {
        this.maxSizeMb = maxSizeMb;
        this.countTrigger = CountTrigger.of(maxCount);
    }

    @Override
    public TriggerResult onElement(A element, long timestamp, W window, TriggerContext ctx) throws Exception {
        var countStatus = countTrigger.onElement(element, timestamp, window, ctx);
        var currentSize = ctx.getPartitionedState(sizeStateDesc);

        TriggerResult finalStatus;

        if (countStatus.isFire()) {
            finalStatus = countStatus;
        } else {
            var size = element.getSize();
            currentSize.add(size);
            var currentSizeInMbs = currentSize.get() / 1_000_000;
            if (currentSizeInMbs >= maxSizeMb) {
                logger.debug("Batch size limit ({}) reached. Releasing batch of size {}", maxSizeMb, currentSizeInMbs);
                finalStatus = TriggerResult.FIRE;
            } else {
                finalStatus = TriggerResult.CONTINUE;
            }
        }

        if (finalStatus.isFire()) {
            currentSize.clear();
        }

        return finalStatus;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return countTrigger.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return countTrigger.onEventTime(time, window, ctx);
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        countTrigger.clear(window, ctx);
        ctx.getPartitionedState(sizeStateDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return countTrigger.canMerge();
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(sizeStateDesc);
        countTrigger.onMerge(window, ctx);
    }

    static private class Sum implements ReduceFunction<Long> {
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }

    public static final class Builder<A extends BigQueryRecord, W extends Window> {
        private int count;
        private int sizeInMb;
        private Duration timeout;
        private boolean resetTimerOnNewRecord = false;

        public Builder<A, W> withCount(int count) {
            this.count = count;
            return this;
        }

        public Builder<A, W> withSizeInMb(int sizeInMb) {
            this.sizeInMb = sizeInMb;
            return this;
        }

        public Builder<A, W> withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder<A, W> withResetTimerOnNewRecord(boolean reset) {
            this.resetTimerOnNewRecord = reset;
            return this;
        }

        public Trigger<A, W> build() {
            return PurgingTrigger.of(
                    ProcessingTimeoutTrigger.of(
                            new BatchTrigger<>(this.count, this.sizeInMb),
                            timeout,
                            resetTimerOnNewRecord,
                            true
                    )
            );
        }
    }
}
