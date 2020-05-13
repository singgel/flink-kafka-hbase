package com.singgel.bigdata.flinksinkhbase;

import com.singgel.bigdata.flinksinkhbase.common.HbaseUtil;
import com.singgel.bigdata.flinksinkhbase.common.JoinTable;
import com.singgel.bigdata.flinksinkhbase.common.ValueFormat;
import com.singgel.bigdata.flinksinkhbase.config.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/31 下午12:29
 * \
 */

@Slf4j
public class HbaseSink extends RichSinkFunction<ObjectNode> implements CheckpointedFunction {

    private final JobConfig jobConfig;

    private HbaseUtil hbaseUtil;

    private long currentTime = System.currentTimeMillis();

    /**
     * 在flink任务自动重试时，会先恢复state中的数据；如果是cancel掉flink任务，重新手动提交，则state会清空
     */
    private transient ListState<ObjectNode> checkpointedState;

    private List<ObjectNode> nodes = new ArrayList<>();

    private static ObjectMapper MAPPER = new ObjectMapper();

    private StringBuilder sbLog = new StringBuilder();

    public HbaseSink(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.hbaseUtil = new HbaseUtil(jobConfig.getHbaseConfig());
    }

    /**
     * 在手动cancel和程序内部出错重试时都会触发close方法，在close方法中将nodes中的数据先flush，防止在两次写入之间，checkpoint点之前的数据丢失
     * 但会有数据重复，checkpoint点之后到发生故障时的数据会重复，如下示意图：
     * <pre>
     *                       flush
     *                        ^
     *                       /
     *            +--------------------------+
     * ------write------checkpoint-----------down----
     *                           +-----------+
     *                                 ^
     *                                /
     *                           will repeat
     * </pre>
     * <p>
     * 但对于写入具有幂等性的业务，数据重复写入不会影响结果
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        log.debug("execute sink close method");
        super.close();
        batchFlush();
        if (this.hbaseUtil.getConnection() != null) {
            try {
                this.hbaseUtil.getConnection().close();
            } catch (Exception e) {
                log.warn("connection close failed. error:{} ", e.getMessage());
            }
        }
    }

    /**
     * 每条记录调用一次此方法
     *
     * @param node
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ObjectNode node, Context context) throws Exception {
        String partition = node.get("metadata").get("partition").asText();
        String offset = node.get("metadata").get("offset").asText();
        String value = node.get("value").asText();
        log.debug("partition->{}|offset->{}|value->{}", partition, offset, value);
        nodes.add(node);
        if (nodes.size() >= jobConfig.getHbaseConfig().getBatchCount() ||
                (System.currentTimeMillis() - currentTime > jobConfig.getHbaseConfig().getInterval() && nodes.size() > 0)) {
            batchFlush();
        }
    }

    /**
     * 将{@link HbaseSink#nodes}中的数据批量写入
     *
     * @throws IOException
     */
    private void batchFlush() throws IOException {
        long start = System.currentTimeMillis();
        List<Put> puts = convertBatch(nodes);
        if (puts.size() == 0) {
            return;
        }
        long startPut = System.currentTimeMillis();
        hbaseUtil.putBatchData(jobConfig.getTableName(), puts);
        long end = System.currentTimeMillis();
        sbLog.append(String.format(" | batch_put(%d) cost %d ms", puts.size(), end - startPut));
        sbLog.append(String.format(" | batch_total(%d) cost %d ms", puts.size(), end - start));
        sbLog.append(String.format(" | per record cost %d ms", (end - start) / puts.size()));
        log.debug(sbLog.toString());
        currentTime = System.currentTimeMillis();
        sbLog = new StringBuilder();
        nodes.clear();
        checkpointedState.clear();
    }

    /**
     * 批量处理
     *
     * @param objectNodes 一批数据
     * @return 返回批量的Put
     */
    private List<Put> convertBatch(List<ObjectNode> objectNodes) throws IOException {
        Map<Put, Map<String, String>> puts = new HashMap<>(objectNodes.size());
        //存储每个需要join的表中这个批次的rowkey的值
        Map<String, Set<String>> joinKeys = new HashMap<>(objectNodes.size());

        for (ObjectNode node : objectNodes) {
            Map<String, String> keyValues = getKeyValues(node);

            //获取拼接的rowKey
            List<String> rowKeyValues = new ArrayList<>();
            jobConfig.getRowKeyColumns().forEach(e -> rowKeyValues.add(keyValues.get(e)));
            if (rowKeyValues.stream().anyMatch(Objects::isNull)) {
                //如果组合rowKey的字段中有null，则过滤掉此记录
                log.warn("columns which consist of rowKey has null value");
                continue;
            }
            String rowKey = String.join(jobConfig.getRowKeyDelimiter(), rowKeyValues);
            Put put = new Put(Bytes.toBytes(rowKey));

            //获取这个joinTable表中这个批次所有需要join的key
            for (JoinTable joinTable : jobConfig.getJoinTables()) {

                joinKeys.compute(joinTable.getTableName(), (k, v) -> {
                    //keyValues.get(joinTable.getJoinKey()的值有可能为null,需做空判断
                    if (keyValues.get(joinTable.getJoinKey()) != null) {
                        if (v == null) {
                            v = new HashSet<>();
                            v.add(keyValues.get(joinTable.getJoinKey()));
                        } else {
                            v.add(keyValues.get(joinTable.getJoinKey()));
                        }
                    }
                    return v;
                });
            }


            //原始topic需要写入的列
            keyValues.forEach((k, v) -> {
                String family = k.split(":")[0];
                String column = k.split(":")[1];
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), v != null ? Bytes.toBytes(v) : null);
            });

            puts.put(put, keyValues);
        }

        //当需要join时执行下面操作
        for (JoinTable joinTable : jobConfig.getJoinTables()) {
            //取出这个joinTable表中这个批次所有需要join的key
            Set<String> keys = joinKeys.get(joinTable.getTableName());
            List<Get> gets = new ArrayList<>();
            //将key和result一一对应
            Map<String, Result> keyResults = new HashMap<>(keys.size());

            //生成需要批量get的List<Get>
            keys.forEach(e -> {
                Get get = new Get(Bytes.toBytes(e));
                joinTable.getColumnsMapping().forEach((k, v) -> {
                    get.addColumn(Bytes.toBytes(k.split(":")[0]), Bytes.toBytes(k.split(":")[1]));
                });
                gets.add(get);
            });


            long start = System.currentTimeMillis();
            //执行批量get
            Result[] results = hbaseUtil.batchGet(joinTable.getTableName(), gets);
            for (Result result : results) {
                if (result != null) {
                    keyResults.put(Bytes.toString(result.getRow()), result);
                }
            }
            long end = System.currentTimeMillis();

            sbLog.append(String.format("| batch_get %s(%d) %d ms", joinTable.getTableName(), keys.size(), (end - start)));
            //对之前原始写入的每个put,获取这个表需要join的rowKey的result,然后将result中的值根据joinTable的配置添加到put的对应列中
            puts.forEach((put, keyValues) -> {
                Result result = keyResults.get(keyValues.get(joinTable.getJoinKey()));
                if (result != null) {
                    joinTable.getColumnsMapping().forEach((k, v) -> {
                        byte[] columnValue = result.getValue(Bytes.toBytes(k.split(":")[0]), Bytes.toBytes(k.split(":")[1]));
                        put.addColumn(Bytes.toBytes(v.split(":")[0]), Bytes.toBytes(v.split(":")[1]), columnValue);
                    });
                }
            });
        }


        return new ArrayList<>(puts.keySet());
    }

    private Put convert(ObjectNode node) throws IOException {

        Map<String, String> keyValues = getKeyValues(node);

        //获取拼接的rowKey
        List<String> rowKeyValues = new ArrayList<>();

        jobConfig.getRowKeyColumns().forEach(e -> rowKeyValues.add(keyValues.get(e)));
        String rowKey = String.join(jobConfig.getRowKeyDelimiter(), rowKeyValues);

        Put put = new Put(Bytes.toBytes(rowKey));

        //原始topic需要写入的列
        keyValues.forEach((k, v) -> {
            String family = k.split(":")[0];
            String column = k.split(":")[1];
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(v));
        });

        //需要join的table
        for (JoinTable joinTable : jobConfig.getJoinTables()) {
            byte[] joinKey = Bytes.toBytes(keyValues.get(joinTable.getJoinKey()));
            Get get = new Get(joinKey);
            joinTable.getColumnsMapping().forEach((k, v) -> {
                get.addColumn(Bytes.toBytes(k.split(":")[0]), Bytes.toBytes(k.split(":")[1]));
            });
            //获取此rowKey所有的列值
            Result result = hbaseUtil.singleGet(joinTable.getTableName(), get);
            //如果result为空，则不需处理
            if (result != null) {
                joinTable.getColumnsMapping().forEach((k, v) -> {
                    byte[] columnValue = result.getValue(Bytes.toBytes(k.split(":")[0]), Bytes.toBytes(k.split(":")[1]));
                    put.addColumn(Bytes.toBytes(v.split(":")[0]), Bytes.toBytes(v.split(":")[1]), columnValue);
                });
            }
        }
        return put;
    }

    /**
     * 根据配置中给定的列值对应关系，将每条消息解析成<key,value>格式，key为配置中指定的列名(包含列簇)
     * 目前支持两种消息格式：CSV和JSON格式的字符串型数据,
     * 在处理消息时，kafka消息的key默认会被集成到value中， 对于CSV格式，kafka消息的key处在index=0的位置；对于JSON格式，kafka消息的key对应默认的kafka_key字段
     *
     * @param node flink接入的kafka消息
     * @return 返回字段名称对应的值
     */
    private Map<String, String> getKeyValues(ObjectNode node) {
        Map<String, String> indexColumns = jobConfig.getIndexColumnMapping();
        String key = node.get("key") == null ? "" : node.get("key").asText();
        String value = node.get("value") == null ? "" : node.get("value").asText();


        Map<String, String> keyValues = new HashMap<>(8);

        ValueFormat valueFormat = jobConfig.getKafkaConfig().getValueFormat();
        switch (valueFormat) {
            case CSV:
                //将key和value拼接起来，配置时kafka的key值作为下标的第0个
                String input = key + jobConfig.getKafkaConfig().getDelimiter() + value;
                String[] columnValues = StringUtils.splitPreserveAllTokens(input, jobConfig.getKafkaConfig().getDelimiter());

                //将index对应的列值写入对应的列名下，列名包含了列簇名，形如：family:qualifier
                for (Map.Entry<String, String> entry : indexColumns.entrySet()) {
                    try {
                        keyValues.put(entry.getValue(), columnValues[Integer.valueOf(entry.getKey())]);
                    } catch (Exception e) {

                        log.warn("index {} out of boundary.", entry.getKey(), e);
                    }
                }

                break;
            case JSON:
            default:
                //将kafka的key加入node，统一处理
                try {

                    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(value);

                    jsonNode.put("kafka_key", key);

                    //将配置中指定的列值写入对应的列名下，列名包含了列簇名，形如：family:qualifier
                    indexColumns.forEach((k, v) -> {
                        if (jsonNode.get(k) != null) {
                            keyValues.put(v, jsonNode.get(k).asText());
                        }
                    });
                } catch (IOException e) {
                    keyValues.clear();
                    indexColumns.forEach((k, v) -> keyValues.put(v, null));
                    String partition = node.get("metadata").get("partition").asText();
                    String offset = node.get("metadata").get("offset").asText();
                    String topic = node.get("metadata").get("topic").asText();
                    log.warn("this json record failed.topic->{},partition->{},offset->{},value->{}", topic, partition, offset, value);
                }
                break;
        }
        return keyValues;
    }

    /**
     * 执行频率和{@link FlinkRunner}中指定的checkpoint间隔一致
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (ObjectNode element : nodes) {
            checkpointedState.add(element);
        }
        log.debug("execute snapshot at {}", System.currentTimeMillis());
    }

    /**
     * 在程序内部出错重启时，如果调用了snapshotState方法，则会恢复checkpointedState中的数据，如果是手动cancel或重试几次失败后重新提交任务，此时
     * 的checkpointedState会是新的对象，里面没有数据
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<ObjectNode> descriptor =
                new ListStateDescriptor<>(
                        "hbase-sink-cp",
                        TypeInformation.of(new TypeHint<ObjectNode>() {
                        }));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (ObjectNode element : checkpointedState.get()) {
                nodes.add(element);
            }
        }
        log.info("initialState {}  record", nodes.size());
    }
}
