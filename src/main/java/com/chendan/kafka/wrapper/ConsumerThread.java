package com.chendan.kafka.wrapper;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThread<K, V> implements Runnable {
    public static enum State {
        INIT, STARTED, SUBSCRIBED, STOPPED
    }

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Consumer<K, V> consumer;
    private final KafkaProperties props;
    private final List<String> topics;
    private State state = State.INIT;

    public ConsumerThread(KafkaProperties props, List<String> topics) {
        this.props = props;
        this.consumer = new Consumer<K, V>(props);
        this.topics = topics;
        this.state = State.INIT;
    }

    public ConsumerThread(
            String bootstrapServers, String groupId, String clientId,
            KafkaProperties.KeyValSerializationType keyDeseri,
            KafkaProperties.KeyValSerializationType valDeseri,
            List<String> topics) {
        this(bootstrapServers, groupId, clientId,
                keyDeseri, valDeseri,
                topics, false);
    }

    public ConsumerThread(
            String bootstrapServers, String groupId, String clientId,
            KafkaProperties.KeyValSerializationType keyDeseri,
            KafkaProperties.KeyValSerializationType valDeseri,
            List<String> topics, boolean from_beginning) {
        this.props = new KafkaProperties();
        this.props.putDefaultConsumerProps(
                bootstrapServers, groupId, clientId)
                .putKeyValDeserializer(keyDeseri, valDeseri);
        if (from_beginning) {
            this.props.putAutoOffsetReset("earliest");
        }
        this.consumer = new Consumer<K, V>(props);
        this.topics = topics;
        this.state = State.INIT;
    }


    public void run() {
        try {
            this.state = State.STARTED;
            consumer.subscribe(topics);
            while (!closed.get()) {
                boolean commit = true;
                ConsumerRecords<K, V> records = consumer.poll(100);
                try {
                    onRecords(records);
                } catch (Exception e) {
                    commit = false;
                }
                if (commit) {
                    consumer.commitSync();
                }
                if (this.state == State.STARTED) {
                    this.state = State.SUBSCRIBED;
                }
            }
            this.state = State.STOPPED;
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
            // ignore for shutdown
        } finally {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    public void onRecords(ConsumerRecords<K, V> records) throws Exception {
        // To be overridden
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public ConsumerThread.State getState() {
        return state;
    }
}
