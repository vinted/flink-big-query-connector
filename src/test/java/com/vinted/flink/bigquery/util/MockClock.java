package com.vinted.flink.bigquery.util;

import org.mockito.Mockito;

import java.io.Serializable;
import java.time.Clock;
import java.util.ArrayList;

public class MockClock implements Serializable {
    private static Clock clock = Mockito.mock(Clock.class);

    public static void reset() {
        Mockito.reset(clock);
    }

    public Clock get() {
        return MockClock.clock;
    }

    public void givenCurrentMillis(long... ms) {
        var list = new ArrayList<Long>();
        for (long i : ms) {
            list.add(i);
        }

        long first = list.get(0);
        Long[] rest = list.subList(1, list.size()).toArray(Long[]::new);
        Mockito.when(clock.millis()).thenReturn(first, rest);
    }
}
