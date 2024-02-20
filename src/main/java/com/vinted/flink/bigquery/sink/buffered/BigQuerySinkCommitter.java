package com.vinted.flink.bigquery.sink.buffered;

import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.protobuf.Int64Value;
import com.vinted.flink.bigquery.client.ClientProvider;
import io.grpc.Status;
import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;

public class BigQuerySinkCommitter implements Committer<BigQueryCommittable> {
    private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkCommitter.class);
    private ClientProvider<?> clientProvider;

    public BigQuerySinkCommitter(ClientProvider<?> clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void commit(Collection<CommitRequest<BigQueryCommittable>> committables) {
        var traceId = UUID.randomUUID().toString();
        committables.forEach(committable -> {
            var offsetToCommit = committable.getCommittable().getOffset() - 1; // Advance the cursor to the latest record.
            logger.debug(createLogMessage(traceId, committable.getCommittable(), "Committing offset"));
            var request = FlushRowsRequest.newBuilder()
                    .setWriteStream(committable.getCommittable().getStreamName())
                    .setOffset(Int64Value.of(offsetToCommit))
                    .build();
            try {
                clientProvider.getClient().flushRows(request);
            } catch (Throwable t) {
                var status = Status.fromThrowable(t);
                switch (status.getCode()) {
                    case ALREADY_EXISTS: {
                        logger.warn("Trace-id {} Rows offset already exists",traceId, t);
                        committable.signalAlreadyCommitted();
                        break;
                    }
                    default: {
                        logger.error(createLogMessage(traceId, committable.getCommittable(), "Commit failed. " + t.getMessage()), t);
                        committable.signalFailedWithUnknownReason(t);
                        break;
                    }
                }
            }
        });
    }

    private String createLogMessage(String traceId, BigQueryCommittable commit, String title) {
        return String.format("Trace-id %s %s\nstream: %s\ncommit offset: %s",
                traceId, title, commit.getStreamName(), commit.getOffset()
        );
    }


    @Override
    public void close() throws Exception {
    }
}
