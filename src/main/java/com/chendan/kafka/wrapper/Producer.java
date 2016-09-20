package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

public class Producer<K, V> {
    private KafkaProducer<K, V> producer = null;

    public Producer(KafkaProperties props) {
        Preconditions.checkNotNull(props);
        producer = new KafkaProducer<K, V>(props.buildProperties());
    }

    public RecordMetadata sendSync(String topic, K key, V val)
            throws InterruptedException, ExecutionException {
        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, val);
        return sendSync(record);
    }

    public RecordMetadata sendSync(ProducerRecord<K, V> record)
            throws InterruptedException, ExecutionException {
        return sendAsync(record, null).get();
    }

    public Future<RecordMetadata> sendAsync(String topic, K key,
                                            V val) {
        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, val);
        return sendAsync(record, null);
    }

    public Future<RecordMetadata> sendAsync(String topic, K key,
                                            V val,
                                            Callback cb) {
        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, val);
        return sendAsync(record, cb);
    }

    public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record,
                                            Callback cb) {
        return producer.send(record, cb);
    }

    public void close() {
        Preconditions.checkState(producer != null);
        producer.close();

    }

    public void flush() {
        Preconditions.checkState(producer != null);
        producer.flush();
    }

    public List<PartitionInfo> getPartitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }
}
