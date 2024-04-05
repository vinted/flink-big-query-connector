# Flink BigQuery Connector ![Build](https://github.com/vinted/flink-big-query-connector/actions/workflows/gradle.yml/badge.svg) [![](https://jitpack.io/v/com.vinted/flink-big-query-connector.svg)](https://jitpack.io/#com.vinted/flink-big-query-connector)

This project provides a BigQuery sink that allows writing data with exactly-once or at-least guarantees.

## Usage

There are builder classes to simplify constructing a BigQuery sink. The code snippet below shows an example of building a BigQuery sink in Java:

```java
var credentials = new JsonCredentialsProvider("key");

var clientProvider = new BigQueryProtoClientProvider<String>(credentials,
    WriterSettings.newBuilder()
                 .build()
);

var bigQuerySink = BigQueryStreamSink.<String>newBuilder()
    .withClientProvider(clientProvider)
    .withDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .withRowValueSerializer(new NoOpRowSerializer<>())
    .build();
```

Async connector for at least once delivery

```java
var credentials = new JsonCredentialsProvider("key");

var clientProvider = new AsyncClientProvider<String>(credentials,
    WriterSettings.newBuilder()
                 .build()
);

var sink = AsyncBigQuerySink.builder()
        .setRowSerializer(new NoOpRowSerializer<>())
        .setClientProvider(clientProvider)
        .setMaxBatchSize(30)
        .setMaxBufferedRequests(10)
        .setMaxBatchSizeInBytes(10000)
        .setMaxInFlightRequests(4)
        .setMaxRecordSizeInBytes(10000)
        .build();
```

The sink takes in a batch of records. Batching happens outside the sink by opening a window. Batched records need to implement the BigQueryRecord interface.

```java
var trigger = BatchTrigger.<Record, GlobalWindow>builder()
    .withCount(100)
    .withTimeout(Duration.ofSeconds(1))
    .withSizeInMb(1)
    .withResetTimerOnNewRecord(true)
    .build();

var processor = new BigQueryStreamProcessor()
    .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();

source.key(s -> s)
    .window(GlobalWindows.create())
    .trigger(trigger)
    .process(processor);

```

To write to BigQuery, you need to:

- Define credentials
- Create a client provider
- Batch records
- Create a value serializer
- Sink to BigQuery

# Credentials

There are two types of credentials:

- Loading from a file

```java
new FileCredentialsProvider("/path/to/file")
```

- Passing as a JSON string

```java
new JsonCredentialsProvider("key")
```

# Types of Streams

BigQuery supports two types of data formats: json and proto. When creating a stream, you can choose these types by creating the appropriate client and using the builder methods.

- JSON

```java
var clientProvider = new BigQueryJsonClientProvider<String>(credentials,
    WriterSettings.newBuilder()
                 .build()
);

var bigQuerySink = BigQueryStreamSink.<String>newBuilder()
```

- Proto

```java
var clientProvider = new BigQueryProtoClientProvider(credentials,
    WriterSettings.newBuilder()
                 .build()
);

var bigQuerySink = BigQueryStreamSink.<String>newBuilder();
```

# Exactly once

It utilizes a [buffered stream](https://cloud.google.com/bigquery/docs/write-api#buffered_type), managed by the BigQueryStreamProcessor, to assign and process data batches. If a stream is inactive or closed, a new stream is created automatically. The BigQuery sink writer appends and flushes data to the latest offset upon checkpoint commit.

# At least once

Data is written to the [default stream](https://cloud.google.com/bigquery/docs/write-api#default_stream) and handled by the BigQueryStreamProcessor, which batches and sends rows to the sink for processing.

# Serializers

For the proto stream, you need to implement `ProtoValueSerializer`, and for the JSON stream, you need to implement `JsonRowValueSerializer`.

# Metrics

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <th rowspan="8">Stream</th>
        <td>stream_offset</td>
        <td>Current offset for the stream. When using at least once, the offset is always 0</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>batch_count</td>
        <td>Number of records in the appended batch</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>batch_size_mb</td>
        <td>Appended batch size in mb</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>split_batch_count</td>
        <td>Number of times the batch hit the BigQuery limit and was split into two parts</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>
