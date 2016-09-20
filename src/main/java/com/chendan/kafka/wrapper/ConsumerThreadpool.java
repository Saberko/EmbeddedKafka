package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

public class ConsumerThreadpool<K, V> {
    private final List<ConsumerThread<K, V>> threads;
    private ExecutorService executor;

    public ConsumerThreadpool(
            String bootstrapServers, String groupId,
            KafkaProperties.KeyValSerializationType keyDeseri,
            KafkaProperties.KeyValSerializationType valDeseri,
            List<String> topics, int numThreads) {
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.threads = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            KafkaProperties props = new KafkaProperties();
            props.putDefaultConsumerProps(
                    bootstrapServers, groupId, groupId + "_" + i)
                    .putKeyValDeserializer(keyDeseri, valDeseri);
            ConsumerThread thread = new ConsumerThread(props, topics);
            threads.add(thread);
        }
    }

    public ConsumerThreadpool(List<ConsumerThread<K, V>> threads) {
        this.executor = Executors.newFixedThreadPool(threads.size());
        this.threads = threads;
    }

    public void startup() {
        Preconditions.checkState(threads != null);
        for (ConsumerThread<K, V> t : threads) {
            executor.submit(t);
        }
    }

    // Runtime.getRuntime().addShutdownHook(new Thread() {
    //   @Override
    //   public void run() {
    //     consumerThreadpool.shutdown();
    //   }
    // }
    public void shutdown() {
        for (ConsumerThread<K, V> t : threads) {
            t.shutdown();
        }
        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public List<ConsumerThread<K, V>> getConsumerThreads() {
        return threads;
    }
}
