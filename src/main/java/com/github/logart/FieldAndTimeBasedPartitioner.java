package com.github.logart;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {
    private final FieldPartitioner<T> fieldBasedPartitioner = new FieldPartitioner<>();

    @Override
    public void configure(Map<String, Object> config) {
        super.configure(config);
        fieldBasedPartitioner.configure(config);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        String fieldPath = fieldBasedPartitioner.encodePartition(sinkRecord);
        String timePath = super.encodePartition(sinkRecord);

        // Combine field + time
        return fieldPath + "/" + timePath;
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
        String fieldPath = fieldBasedPartitioner.encodePartition(sinkRecord, nowInMillis);
        String timePath = super.encodePartition(sinkRecord, nowInMillis);

        return fieldPath + "/" + timePath;
    }

    @Override
    public List<T> partitionFields() {
        List<T> combinedPartitionFields = new ArrayList<T>();
        combinedPartitionFields.addAll(fieldBasedPartitioner.partitionFields());
        combinedPartitionFields.addAll(super.partitionFields());
        return combinedPartitionFields;
    }
}
