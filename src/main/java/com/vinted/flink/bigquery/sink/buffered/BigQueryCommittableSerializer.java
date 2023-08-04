package com.vinted.flink.bigquery.sink.buffered;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class BigQueryCommittableSerializer implements SimpleVersionedSerializer<BigQueryCommittable> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BigQueryCommittable data) throws IOException {
        try (var baos = new ByteArrayOutputStream(); var out = new DataOutputStream(baos)) {
            out.writeUTF(data.getStreamName());
            out.writeLong(data.getOffset());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BigQueryCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (var bais = new ByteArrayInputStream(serialized); var in = new DataInputStream(bais)) {
            var name = in.readUTF();
            var offset = in.readLong();
            return new BigQueryCommittable(name, offset);
        }
    }
}
