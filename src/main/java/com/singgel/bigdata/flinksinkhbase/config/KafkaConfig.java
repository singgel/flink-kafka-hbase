package com.singgel.bigdata.flinksinkhbase.config;

import com.singgel.bigdata.flinksinkhbase.common.ValueFormat;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午5:21
 * \
 */
public class KafkaConfig implements Serializable {

    /**
     * 数据源kafka的连接
     */
    private String bootstrapServers;

    /**
     * 数据源的topic
     */
    private String topic;

    /**
     * 消费topic的groupId
     */
    private String groupId;

    /**
     * kafka value的类型，目前支持：CSV和JSON
     */
    private ValueFormat valueFormat;

    /**
     * kafka消息value的分割符，当valueFormat为CSV格式时必须指定
     */
    private String delimiter;

    /**
     * 其它配置
     */
    private Map<String, String> optionalProps;


    public KafkaConfig() {
    }


    public KafkaConfig(String bootstrapServers, String topic, String groupId,ValueFormat valueFormat, String splitor, Map<String, String> optionalProps) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.valueFormat = valueFormat;
        this.delimiter = splitor;
        this.optionalProps = optionalProps;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public ValueFormat getValueFormat() {
        return valueFormat;
    }

    public void setValueFormat(ValueFormat valueFormat) {
        this.valueFormat = valueFormat;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Map<String, String> getOptionalProps() {
        return optionalProps;
    }

    public void setOptionalProps(Map<String, String> optionalProps) {
        this.optionalProps = optionalProps;
    }


    public Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", this.bootstrapServers);
        props.setProperty("group.id", this.groupId);
        optionalProps.forEach(props::setProperty);
        return props;
    }
}
