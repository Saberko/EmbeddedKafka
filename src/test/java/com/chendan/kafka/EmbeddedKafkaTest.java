package com.chendan.kafka;


import com.chendan.kafka.wrapper.*;
import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmbeddedKafkaTest {
    private static final int ZK_PORT = 12346;
    private static final int KAFKA_PORT = 56789;
    private EmbeddedKafka broker = null;
    private Producer<String, String> producer = null;
    private Consumer<String, String> consumer = null;

    private void createTopic(String topic) {
        int partitions = 1;
        int replFactor = 1;
        createTopic(topic, partitions, replFactor);
    }

    private void createTopic(String topic,
                             int partitions) {
        int replFactor = 1;
        createTopic(topic, partitions, replFactor);
    }

    private void createTopic(String topic,
                             int partitions,
                             int replFactor) {
        TopicAdmin topicAdmin = new TopicAdmin("localhost:" + ZK_PORT);
        topicAdmin.createTopic(topic, partitions, replFactor);
    }

    private boolean existsTopic(String topic) {
        TopicAdmin topicAdmin = new TopicAdmin("localhost:" + ZK_PORT);
        return topicAdmin.existsTopic(topic);
    }

    private void deleteTopic(String topic) {
        TopicAdmin topicAdmin = new TopicAdmin("localhost:" + ZK_PORT);
        Preconditions.checkState(topicAdmin.existsTopic(topic));
        topicAdmin.deleteTopic(topic);
    }

    private Producer<String, String> createProducer() {
        KafkaProperties props = new KafkaProperties();
        props.putDefaultProducerProps("localhost:" + KAFKA_PORT, "myProducerId")
                .putKeyValSerializer(
                        KafkaProperties.KeyValSerializationType.STRING,
                        KafkaProperties.KeyValSerializationType.STRING);
        return new Producer<String, String>(props);
    }

    private Consumer<String, String> createConsumer() {
        KafkaProperties props = new KafkaProperties();
        props.putDefaultConsumerProps(
                "localhost:" + KAFKA_PORT, "myGroupId", "myConsumerId")
                .putKeyValDeserializer(
                        KafkaProperties.KeyValSerializationType.STRING,
                        KafkaProperties.KeyValSerializationType.STRING);
        return new Consumer<String, String>(props);
    }

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @Before
    public void setUpEach() throws Exception {
        // Needs to be set up and teared down on a per-test basis, otherwise
        // it appears onl pubbing/subbing the first topic works
        Preconditions.checkState(broker == null);
        broker = new EmbeddedKafka(ZK_PORT, KAFKA_PORT);
        broker.startServer();

        Preconditions.checkState(producer == null);
        Preconditions.checkState(consumer == null);
        producer = createProducer();
        consumer = createConsumer();
        Preconditions.checkState(producer != null);
        Preconditions.checkState(consumer != null);
    }

    @After
    public void tearDownEach() throws Exception {
        Preconditions.checkState(producer != null);
        Preconditions.checkState(consumer != null);
        producer.close();
        producer = null;
        consumer.close();
        consumer = null;

        Preconditions.checkState(broker != null);
        broker.stopServer();
        broker = null;
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void simplePubSub() throws Exception {
        String topic = "mytopic";
        assertFalse(existsTopic(topic));
        createTopic(topic);
        assertTrue(existsTopic(topic));

        consumer.subscribe(Arrays.asList(topic));
        // Without this poll, consumer appears to not receiving the msg below
        consumer.poll(1);
        producer.sendSync(topic, "mykey", "myval");
        ConsumerRecords<String, String> records = consumer.poll(100);
        Preconditions.checkState(records != null);
        List<String> msgs = new ArrayList<String>();
        for (ConsumerRecord<String, String> record : records.records(topic)) {
            msgs.add(record.value());
        }
        assertEquals(Arrays.asList("myval"), msgs);
        consumer.commitSync();
        consumer.unsubscribe();

        deleteTopic(topic);
        Thread.sleep(1000);
        assertFalse(existsTopic(topic));
    }
}

