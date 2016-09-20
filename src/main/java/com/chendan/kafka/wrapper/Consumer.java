package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Consumer<K, V> {
    private KafkaConsumer<K, V> consumer = null;

    // List of subscribed topics saved
    private List<String> topics = null;
    // Consumer connection closed
    private boolean closed = false;

    public Consumer(KafkaProperties props) {
        Preconditions.checkNotNull(props);
        consumer = new KafkaConsumer<K, V>(props.buildProperties());
        Preconditions.checkState(consumer != null);
    }

    public Consumer(Properties props) {
        Preconditions.checkNotNull(props);
        consumer = new KafkaConsumer<K, V>(props);
        Preconditions.checkState(consumer != null);
    }

    public Consumer subscribe(List<String> topics) {
        Preconditions.checkNotNull(topics);
        Preconditions.checkState(!closed);
        Preconditions.checkState(this.topics == null);
        this.topics = topics;
        consumer.subscribe(topics);
        return this;
    }

    public Consumer unsubscribe() {
        Preconditions.checkState(topics != null);
        Preconditions.checkState(!closed);
        consumer.unsubscribe();
        topics = null;
        return this;
    }

    public ConsumerRecords<K, V> poll(long timeout) {
        Preconditions.checkState(!closed);
        return consumer.poll(timeout);
    }

    public Consumer commitSync() {
        Preconditions.checkState(!closed);
        consumer.commitSync();
        return this;
    }

    public Consumer commitAsync() {
        Preconditions.checkState(!closed);
        consumer.commitAsync();
        return this;
    }

    public Set<String> subscription() {
        return consumer.subscription();
    }

    public void wakeup() {
        consumer.wakeup();
    }

    public void close() {
        Preconditions.checkState(!closed);
        consumer.close();
        closed = true;
    }
}
