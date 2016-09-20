package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallback implements Callback {
    private long startTime = -1;
    private long endTime = -1;
    private String key;
    private String msg;

    // RecordMetadata fields
    private long offset = -1;
    private int partition = -1;
    private String topic;

    private boolean done = false;

    public ProducerCallback() {
    }

    public ProducerCallback(long startTime, String key, String msg) {
        this.startTime = startTime;
        this.key = key;
        this.msg = msg;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {
        Preconditions.checkState(!done);
        done = true;
        endTime = System.currentTimeMillis();
        if (metadata != null) {
            offset = metadata.offset();
            partition = metadata.partition();
            topic = metadata.topic();
        } else {
            e.printStackTrace();
        }
    }
}
