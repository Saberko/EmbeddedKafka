package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProperties {
    private HashMap<String, String> props = null;

    public KafkaProperties() {
        props = new HashMap<String, String>();
    }

    // You may need to override KEY/VALUE_SERIALIZER_CLASS_CONFIG
    public KafkaProperties putDefaultProducerProps(
            String bootstrapServers, String producerId) {
        Preconditions.checkState(props != null);
        Preconditions.checkArgument(
                bootstrapServers != null && !bootstrapServers.isEmpty());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Size of mem used for buffering records
        // May affect producer throughput
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        // Batch records sent to the same partition
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        return this;
    }

    public enum KeyValSerializationType {
        BYTE_ARRAY, INTEGER, LONG, STRING
    }

    public KafkaProperties putKeyValSerializer(
            KeyValSerializationType keySeri, KeyValSerializationType valSeri) {
        switch (keySeri) {
            case BYTE_ARRAY:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");
                break;
            case INTEGER:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.IntegerSerializer");
                break;
            case LONG:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.LongSerializer");
                break;
            case STRING:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringSerializer");
                break;
        }
        switch (valSeri) {
            case BYTE_ARRAY:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");
                break;
            case INTEGER:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.IntegerSerializer");
                break;
            case LONG:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.LongSerializer");
                break;
            case STRING:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringSerializer");
                break;
        }
        return this;
    }

    public KafkaProperties putKeyValDeserializer(
            KeyValSerializationType keyDeseri,
            KeyValSerializationType valDeseri) {
        switch (keyDeseri) {
            case BYTE_ARRAY:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                break;
            case INTEGER:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.IntegerDeserializer");
                break;
            case LONG:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.LongDeserializer");
                break;
            case STRING:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer");
                break;
        }
        switch (valDeseri) {
            case BYTE_ARRAY:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                break;
            case INTEGER:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.IntegerDeserializer");
                break;
            case LONG:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.LongDeserializer");
                break;
            case STRING:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer");
                break;
        }
        return this;
    }


    public KafkaProperties putDefaultConsumerProps(
            String bootstrapServers, String groupId, String consumerId) {
        Preconditions.checkState(props != null);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
        // Consumer heartbeat timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // Disable auto commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        return this;
    }

    // earliest (from beginning) or latest (default) or none (?)
    public KafkaProperties putAutoOffsetReset(String prop) {
        Preconditions.checkState(props != null);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop);
        return this;
    }

    public KafkaProperties put(String prop, String val) {
        Preconditions.checkState(props != null);
        props.put(prop, val);
        return this;
    }

    public String getClientId() {
        Preconditions.checkState(props != null);
        return props.get(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    public String getGroupId() {
        Preconditions.checkState(props != null);
        return props.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public Properties buildProperties() {
        Preconditions.checkState(props != null);
        Preconditions.checkState(!props.isEmpty());
        Properties properties = new Properties();
        for (Map.Entry<String, String> e : props.entrySet()) {
            properties.put(e.getKey(), e.getValue());
        }
        return properties;
    }
}
