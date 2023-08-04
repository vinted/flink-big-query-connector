package com.vinted.flink.bigquery.sink;

import java.io.Serializable;
import java.util.concurrent.Executor;

public interface ExecutorProvider extends Serializable {

    Executor create();
}
