package com.singgel.bigdata.flinksinkhbase.config;

import java.io.Serializable;
import java.util.Map;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/28 上午9:29
 * \
 */
public class HbaseConfig implements Serializable{

    /**
     * zookeeper机器名，多个用逗号连接
     */
    private String  zookerperQuorum;

    /**
     * 端口，默认2181
     */
    private String port = "2181";

    /**
     * hbase在zookeeper节点的路径
     */
    private String zookeeperZondeParent;

    /**
     * 其它非必需配置
     */
    private Map<String,String> optionalProp;

    /**
     * 批量写入时每批的数量
     */
    private int batchCount;

    /**
     * 每批次的时间间隔，单位：毫秒
     */
    private long interval;

    public HbaseConfig() {
    }

    public HbaseConfig(String zookerperQuorum,
                       String port,
                       String zookeeperZondeParent,
                       int batchCount,
                       long interval,
                       Map<String,String> optionalProp) {
        this.zookerperQuorum = zookerperQuorum;
        this.port = port;
        this.zookeeperZondeParent = zookeeperZondeParent;
        this.batchCount = batchCount;
        this.interval = interval;
        this.optionalProp =optionalProp;
    }

    public String getZookerperQuorum() {
        return zookerperQuorum;
    }

    public void setZookerperQuorum(String zookerperQuorum) {
        this.zookerperQuorum = zookerperQuorum;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getZookeeperZondeParent() {
        return zookeeperZondeParent;
    }

    public void setZookeeperZondeParent(String zookeeperZondeParent) {
        this.zookeeperZondeParent = zookeeperZondeParent;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public Map<String, String> getOptionalProp() {
        return optionalProp;
    }

    public void setOptionalProp(Map<String, String> optionalProp) {
        this.optionalProp = optionalProp;
    }
}
