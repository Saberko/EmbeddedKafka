package com.chendan.kafka.wrapper;

import com.google.common.base.Preconditions;

import scala.collection.Map;
import scala.Option;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class TopicAdmin {
    private String zkConnect;
    private int sessionTimeoutMs = Integer.MAX_VALUE;
    private int connectionTimeoutMs = Integer.MAX_VALUE;

    public TopicAdmin(String zkConnect) {
        this.zkConnect = zkConnect;
        this.sessionTimeoutMs = 10 * 1000;
        this.connectionTimeoutMs = 10 * 1000;
    }

    private ZkClient openZkClient() {
        return new ZkClient(
                zkConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
    }

    private void closeZkClient(ZkClient zkClient) {
        Preconditions.checkNotNull(zkClient);
        zkClient.close();
    }

    private ZkUtils getZkUtils(ZkClient zkClient) {
        Preconditions.checkNotNull(zkClient);
        boolean isSecureKafkaCluster = false;
        return new ZkUtils(zkClient,
                new ZkConnection(zkConnect), isSecureKafkaCluster);
    }

    public boolean existsTopic(String topic) {
        ZkClient zkClient = openZkClient();
        boolean exists = AdminUtils.topicExists(getZkUtils(zkClient), topic);
        closeZkClient(zkClient);
        return exists;
    }

    public void createTopic(String topic, int partitions, int replFactor) {
        createTopic(topic, partitions, replFactor, new Properties());
    }

    public void createTopic(String topic, int partitions, int replFactor,
                            long retentionSec, long retentionGB) {
        Properties topicConfig = new Properties();
        topicConfig.put("retention.ms", "" + retentionSec * 1000L);
        topicConfig.put("retention.bytes", "" + retentionGB * 1024L * 1024L);
        createTopic(topic, partitions, replFactor, topicConfig);
    }

    public void createTopic(String topic, int partitions, int replFactor,
                            Properties topicConfig) {
        ZkClient zkClient = openZkClient();
        AdminUtils.createTopic(getZkUtils(zkClient), topic, partitions,
                replFactor, topicConfig);
        closeZkClient(zkClient);
    }

    //  Does nothing or just mark for deletion unless
    // delete.topic.enable = true in config/server.properties
    public void deleteTopic(String topic) {
        ZkClient zkClient = openZkClient();
        AdminUtils.deleteTopic(getZkUtils(zkClient), topic);
        closeZkClient(zkClient);
    }

    public Properties fetchTopicConfig(String topic) {
        Map<String, Properties> allTopicConfigs = fetchAllTopicConfigs();
        Option<Properties> topicConfigOpt = allTopicConfigs.get(topic);
        Properties topicConfig =
                topicConfigOpt.isEmpty() ? new Properties() : topicConfigOpt.get();
        return topicConfig;
    }

    private Map<String, Properties> fetchAllTopicConfigs() {

        ZkClient zkClient = openZkClient();
        Map<String, Properties> allTopicConfigs =
                AdminUtils.fetchAllTopicConfigs(getZkUtils(zkClient));
        closeZkClient(zkClient);
        return allTopicConfigs;
    }


}
