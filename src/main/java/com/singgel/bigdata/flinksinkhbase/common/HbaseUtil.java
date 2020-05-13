package com.singgel.bigdata.flinksinkhbase.common;

import com.singgel.bigdata.flinksinkhbase.config.HbaseConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午1:48
 * \
 */
@Slf4j
public class HbaseUtil implements Serializable {


    private static Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

    private Configuration configuration;
    private Connection connection;

    public HbaseUtil(HbaseConfig hbaseConfig) {
        this.configuration = HBaseConfiguration.create();
        this.configuration.set("hbase.zookeeper.quorum", hbaseConfig.getZookerperQuorum());
        this.configuration.set("hbase.zookeeper.property.clientPort", hbaseConfig.getPort());
        this.configuration.set("zookeeper.znode.parent", hbaseConfig.getZookeeperZondeParent());
        hbaseConfig.getOptionalProp().forEach((k, v) -> this.configuration.set(k, v));
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * 获取某个rowKey的所有列簇所有列值
     *
     * @param tableName hbase表名
     * @param get       只指定了rowKey的get
     * @return 返回result
     */
    public Result singleGet(String tableName, Get get) throws IOException {

        Result result = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            result = table.get(get);

        } catch (IOException e) {
            log.error("singleGet rowKey:{} get failed", new String(get.getRow()), e);
            throw e;
        }
        return result;
    }

    /**
     * 批量获取
     *
     * @param tableName 表名
     * @param gets      get列表
     * @return
     */
    public Result[] batchGet(String tableName, List<Get> gets) throws IOException {
        Result[] results = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            results = table.get(gets);

        } catch (IOException e) {
            logger.warn("batchGets get failed", e);
            throw e;
        }
        return results;
    }


    /**
     * 向hbase表插入数据
     *
     * @param tableName hbase表名
     * @param put       要插入的put，需指定列簇和列
     */
    public void putData(String tableName, Put put) {
        System.out.println("begin put");
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(put);
            System.out.println("put success");
        } catch (IOException e) {
            logger.warn("rowKey:{} put failed", new String(put.getRow()), e);
        }
    }

    /**
     * 向hbase表批量插入数据
     *
     * @param tableName hbase表名
     * @param puts      要插入的puts，需指定列簇和列
     */
    public void putBatchData(String tableName, List<Put> puts) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(puts);
        } catch (IOException e) {
            log.error("put batch data failed",e);
            throw e;
        }
    }

    /**
     * 准备要写入的hbase表，如果表不存在则创建，并添加列簇，如果存在则添加不存在的列簇
     *
     * @param tableName hbase表名
     * @param families  写入的列簇
     * @throws IOException
     */
    public void prepareTable(String tableName, Iterable<String> families) throws IOException {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(tableName)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                HTableDescriptor hTableDescriptor = table.getTableDescriptor();
                List<String> existFamilies = new ArrayList<>();
                List<String> needAddedFamilies = new ArrayList<>();
                for (HColumnDescriptor fdescriptor : hTableDescriptor.getColumnFamilies()) {
                    existFamilies.add(fdescriptor.getNameAsString());
                }
                for (String family : families) {
                    if (!existFamilies.contains(family)) {
                        needAddedFamilies.add(family);
                    }
                }
                //当有需要新增的列簇时再disable table,增加列簇
                if (needAddedFamilies.size() > 0) {
                    admin.disableTable(tableName);
                    for (String family : needAddedFamilies) {
                        admin.addColumn(tableName, new HColumnDescriptor(family));
                    }
                    admin.enableTable(tableName);
                }
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                for (String family : families) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(hTableDescriptor);
            }
            admin.close();
            connection.close();
        } catch (IOException e) {
            log.error("prepare table failed! check the table and columnFamilies.",e);
            throw e;
        }


    }

}
