package com.vinted.flink.bigquery.sink.async;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.vinted.flink.bigquery.sink.buffered.BigQueryCommittable;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.*;

public class StreamRequestSerializer extends AsyncSinkWriterStateSerializer<StreamRequest> {
    @Override
    protected void serializeRequestToStream(StreamRequest request, DataOutputStream dataOutputStream) throws IOException {
            dataOutputStream.writeUTF(request.getStream());
            dataOutputStream.writeUTF(request.getTable().getProject());
            dataOutputStream.writeUTF(request.getTable().getDataset());
            dataOutputStream.writeUTF(request.getTable().getTable());
            var bytes = request.getData().toByteArray();
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
            dataOutputStream.flush();
    }

    @Override
    protected StreamRequest deserializeRequestFromStream(long requestSize, DataInputStream dataInputStream) throws IOException {
            var name = dataInputStream.readUTF();
            var project = dataInputStream.readUTF();
            var dataset = dataInputStream.readUTF();
            var table = dataInputStream.readUTF();
            var dataLength = dataInputStream.readInt();
            var data = dataInputStream.readNBytes(dataLength);
            var tableId = TableId.of(project, dataset, table);
            try {
                return new StreamRequest(name, tableId, ProtoRows.parseFrom(data));
            } catch (Exception e) {
                throw new RuntimeException("name=" + name + " project=" + project + " dataset=" + dataset + " table=" + table + " " + e.getMessage());
            }

    }

    @Override
    public int getVersion() {
        return 1;
    }
}
