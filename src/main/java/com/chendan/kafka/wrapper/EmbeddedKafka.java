package com.chendan.kafka.wrapper;


import com.google.common.base.Preconditions;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class EmbeddedKafka {
    private int zkPort;
    private int brokerPort;

    private KafkaServerStartable broker;
    private Properties brokerConfig;
    private TestingServer zkServer;

    private boolean running = false;

    public EmbeddedKafka(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        brokerConfig = new Properties();
    }

    public void startServer() throws Exception {
        if (running) {
            return;
        }
        zkServer = new TestingServer(zkPort);
        File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();

        Preconditions.checkState(brokerConfig != null);
        Preconditions.checkState(brokerConfig.isEmpty());

        brokerConfig.setProperty("zookeeper.connect", zkServer.getConnectString());
        brokerConfig.setProperty("broker.id", "1");
        brokerConfig.setProperty("host.name", "localhost");
        brokerConfig.setProperty("port", Integer.toString(brokerPort));
        brokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        brokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        brokerConfig.setProperty("auto.create.topics.enable", "false");
        brokerConfig.setProperty("delete.topic.enable", "true");
        broker = new KafkaServerStartable(new KafkaConfig(brokerConfig));
        try {
            broker.startup();
        } catch (Exception e) {
            e.printStackTrace();
        }
        running = true;
    }

    public void stopServer() throws Exception {
        if (!running) {
            return;
        }
        broker.shutdown();
        zkServer.stop();
    }
}
