package com.vinted.flink.bigquery.sink.async;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.vinted.flink.bigquery.metric.BigQueryStreamMetrics;
import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.sink.AppendException;
import com.vinted.flink.bigquery.sink.defaultStream.BigQueryDefaultSinkWriter;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AsyncBigQuerySinkWriter<A> extends AsyncSinkWriter<Rows<A>, StreamRequest> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkWriter.class);
    private final AsyncClientProvider clientProvider;

    private final Executor appendExecutor;

    protected transient Map<String, StreamWriter> streamMap = new ConcurrentHashMap<>();

    protected BigQueryWriteClient client;

    public AsyncBigQuerySinkWriter(AsyncClientProvider clientProvider, ElementConverter<Rows<A>, StreamRequest> elementConverter, Sink.InitContext context, AsyncSinkWriterConfiguration configuration, Collection<BufferedRequestState<StreamRequest>> bufferedRequestStates) {
        super(elementConverter, context, configuration, bufferedRequestStates);
        appendExecutor = Executors.newFixedThreadPool(4);
        this.clientProvider = clientProvider;
    }

    protected final StreamWriter streamWriter(String traceId, String streamName, TableId table) {
        return streamMap.computeIfAbsent(streamName, name -> {
            logger.trace("Trace-id {} Stream not found {}. Creating new stream", traceId, streamName);
            // Stream name can't contain index
            return this.clientProvider.getWriter(streamName, table);
        });
    }

    protected final void recreateStreamWriter(String traceId, String streamName, String writerId, TableId table) {
        logger.info("Trace-id {} Closing all writers for {}", traceId, streamName);
        try {
            flush(true);
            streamMap.replaceAll((key, writer) -> {
                var newWriter = writer;
                if  (writer.getWriterId().equals(writerId)) {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        logger.trace("Trace-id {} Could not close writer for {}", traceId, streamName);
                    }
                    newWriter = this.clientProvider.getWriter(streamName, table);
                }
                return newWriter;
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void submitRequestEntries(List<StreamRequest> list, Consumer<List<StreamRequest>> consumer) {
        var traceId = UUID.randomUUID().toString();
        var parent = this;

       CompletableFuture.runAsync(() -> {
            var counter = new CountDownLatch(list.size());
            var result = new ConcurrentLinkedDeque<StreamRequest>();
            list.forEach(request -> {
                var writer = streamWriter(traceId, request.getStream(), request.getTable());
                if (writer.isClosed()) {
                    logger.warn("Trace-id {}, StreamWrite is closed. Recreating stream for {}", traceId, request.getStream());
                    recreateStreamWriter(traceId, request.getStream(), writer.getWriterId(), request.getTable());
                    writer = streamWriter(traceId, request.getStream(), request.getTable());
                }
                logger.trace("Trace-id {}, Writing rows stream {} to steamWriter for {} writer id {}", traceId, request.getStream(), writer.getStreamName(), writer.getWriterId());
                try {
                    var apiFuture = writer.append(request.getData());
                    ApiFutures.addCallback(apiFuture, new AppendCallBack<>(parent, writer.getWriterId(), traceId, request, result, counter), appendExecutor);
                } catch (Throwable t) {
                    logger.error("Trace-id {}, StreamWriter failed to append {}", traceId, t.getMessage());
                    counter.countDown();
                    getFatalExceptionCons().accept(new RuntimeException(t));
                }
            });
            try {
                counter.await();
                var finalResult = new ArrayList<>(result);
                consumer.accept(finalResult);
            } catch (InterruptedException e) {
                getFatalExceptionCons().accept(new RuntimeException(e));
            }
        }, appendExecutor);
    }

    @Override
    protected long getSizeInBytes(StreamRequest StreamRequest) {
        return StreamRequest.getData().getSerializedSize();
    }


    static class AppendCallBack<A> implements ApiFutureCallback<AppendRowsResponse> {
        private final AsyncBigQuerySinkWriter<A> parent;
        private final StreamRequest request;

        private final String writerId;
        private final String traceId;

        private final ConcurrentLinkedDeque<StreamRequest> out;

        private final CountDownLatch counter;

        public AppendCallBack(AsyncBigQuerySinkWriter<A> parent, String writerId, String traceId, StreamRequest request, ConcurrentLinkedDeque<StreamRequest> out, CountDownLatch counter) {
            this.parent = parent;
            this.writerId = writerId;
            this.traceId = traceId;
            this.request = request;
            this.out = out;
            this.counter = counter;
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
//            this.retries.accept(List.of());
            counter.countDown();
        }


        @Override
        public void onFailure(Throwable t) {
            var status = Status.fromThrowable(t);
            switch (status.getCode()) {
                case INTERNAL:
                case CANCELLED:
                case FAILED_PRECONDITION:
                case DEADLINE_EXCEEDED:
                    out.add(request);
                    break;
                case ABORTED:
                case UNAVAILABLE: {
                    this.parent.recreateStreamWriter(traceId, request.getStream(), writerId, request.getTable());
                    out.add(request);
                    break;
                }
                case INVALID_ARGUMENT:
                    if (t.getMessage().contains("INVALID_ARGUMENT: MessageSize is too large.")) {
//                        Optional.ofNullable(this.parent.metrics.get(rows.getStream())).ifPresent(BigQueryStreamMetrics::incSplitCount);
                        logger.warn("Trace-id {} MessageSize is too large. Splitting batch", traceId);
                        var data = request.getData().getSerializedRowsList();
                        var first = data.subList(0, data.size() / 2);
                        var second = data.subList(data.size() / 2, data.size());
                        try {
                            var retryRequest = List.of(
                                    new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(first).build()),
                                    new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(second).build())
                            );
                            out.add(new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(first).build()));
                            out.add(new StreamRequest(request.getStream(), request.getTable(), ProtoRows.newBuilder().addAllSerializedRows(second).build()));
                        } catch (Throwable e) {
                            this.parent.getFatalExceptionCons().accept(new RuntimeException(e));
                        }
                    } else {
                        logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
                        this.parent.getFatalExceptionCons().accept(new RuntimeException("failed"));
                    }
                    break;
                case UNKNOWN:
                    if (t instanceof Exceptions.MaximumRequestCallbackWaitTimeExceededException || t.getCause() instanceof Exceptions.MaximumRequestCallbackWaitTimeExceededException) {
                        logger.info("Trace-id {} request timed out: {}", traceId, t.getMessage());
                        this.parent.recreateStreamWriter(traceId, request.getStream(), writerId, request.getTable());
                        out.add(request);
                    } else {
                        logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
//                        this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
                        this.parent.getFatalExceptionCons().accept(new RuntimeException("failed"));
                    }
                    break;
                default:
                    logger.error("Trace-id {} Received error {} with status {}", traceId, t.getMessage(), status.getCode());
//                    this.parent.appendAsyncException = new AppendException(traceId, rows, retryCount, t);
                    this.parent.getFatalExceptionCons().accept(new RuntimeException("failed"));
            }

            counter.countDown();
        }
    }
}
