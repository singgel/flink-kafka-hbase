package com.singgel.bigdata.flinksinkhbase.config;


import com.singgel.bigdata.flinksinkhbase.common.JoinTable;
import com.singgel.bigdata.flinksinkhbase.common.ValueFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午1:50
 * \
 */
public class JobConfig implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final long CHECKPOINT_INTERVAR = 10000L;

    /**
     * kafka消息(整合key和value的值)按分隔符分隔后index和列信息的对应关系
     */
    private Map<String, String> indexColumnMapping = new HashMap<>();


    /**
     * 多列组合成rowKey时指定的连接字符
     */
    private String rowKeyDelimiter;

    /**
     * 组成rowKey的列
     */
    private List<String> rowKeyColumns;

    /**
     * 要写入hbase表的表名，称为"主表"
     */
    private String tableName;

    /**
     * kafka源配置
     */
    private KafkaConfig kafkaConfig;


    /**
     * hbase基本配置
     */
    private HbaseConfig hbaseConfig;

    /**
     * flink job名字，同apollo配置的key
     */
    private String jobName;

    /**
     * 通过实时平台前端界面{@link \http://10.10.20.81:7878/realtime/platform/jobList} 启动flink任务时指定的并发数
     */
    private int parallelism;

    /**
     * 通过实时平台前端界面{@link \http://10.10.20.81:7878/realtime/platform/jobList} 启动flink任务时指定的jar包名称
     */
    private String jarName;

    public JobConfig() {
    }

    public JobConfig(Map<String, String> indexColumnMapping,
                     String rowKeyDelimiter,
                     List<String> rowKeyColumns,
                     String tableName,
                     KafkaConfig kafkaConfig,
                     HbaseConfig hbaseConfig,
                     List<JoinTable> joinTables,
                     String jobName,
                     int parallelism,
                     String jarName) {
        this.indexColumnMapping = indexColumnMapping;
        this.rowKeyDelimiter = rowKeyDelimiter;
        this.rowKeyColumns = rowKeyColumns;
        this.tableName = tableName;
        this.kafkaConfig = kafkaConfig;
        this.hbaseConfig = hbaseConfig;
        this.joinTables = joinTables;
        this.jobName = jobName;
        this.parallelism = parallelism;
        this.jarName = jarName;
    }


    /**
     * 校验:
     * 1. joinTable中的joinKey需要在主表的列中存在
     * 2. indexColumnMapping的values不能重复
     * 3. joinTable中需要写入到mainTable中的列不能与mainTable原有的列重合
     * 4. 当valueFormat=CSV时，delimiter必须要指定
     * 5. hbase批量写入间隔要不能大于flink任务checkpoint的间隔
     */
    public void validate() throws IllegalArgumentException {
        for (JoinTable joinTable : this.joinTables) {
            if (indexColumnMapping.values().stream().noneMatch(e -> e.contains(joinTable.getJoinKey()))) {
                throw new IllegalArgumentException(String.format("%s does not exist in the columns of main table %s", joinTable.getJoinKey(), tableName));
            }
        }
        if (indexColumnMapping.keySet().size() != indexColumnMapping.values().size()) {
            throw new IllegalArgumentException(String.format("the column in a family must not be duplicate"));
        }
        for (JoinTable joinTable : joinTables) {
            if (joinTable.getColumnsMapping().values().stream().anyMatch(e -> indexColumnMapping.values().contains(e))) {
                throw new IllegalArgumentException(String.format("some column in joinTable:%s has existed in mainTable:%s. Please check!", joinTable, tableName));
            }
        }
        if (kafkaConfig.getValueFormat() == ValueFormat.CSV && StringUtils.isEmpty(kafkaConfig.getDelimiter())) {
            throw new IllegalArgumentException(String.format("the delimiter must be given when the valueFormat is CSV"));
        }
        if(hbaseConfig.getInterval()>CHECKPOINT_INTERVAR){
            hbaseConfig.setInterval(CHECKPOINT_INTERVAR);
        }

    }

    /**
     * 获取mainTable需要写入的列簇
     *
     * @return 列簇的集合
     */
    public Set<String> families() {
        Set<String> mainTableFamilies = this.indexColumnMapping.values().stream().map(e -> e.split(":")[0]).collect(Collectors.toSet());
        for (JoinTable joinTable : joinTables) {
            mainTableFamilies.addAll(joinTable.getColumnsMapping().values().stream().map(e -> e.split(":")[0]).collect(Collectors.toSet()));
        }
        return mainTableFamilies;
    }


    public Map<String, String> getIndexColumnMapping() {
        return indexColumnMapping;
    }

    public void setIndexColumnMapping(Map<String, String> indexColumnMapping) {
        this.indexColumnMapping = indexColumnMapping;
    }


    private List<JoinTable> joinTables = new ArrayList<>();

    public String getRowKeyDelimiter() {
        return rowKeyDelimiter;
    }

    public void setRowKeyDelimiter(String rowKeyDelimiter) {
        this.rowKeyDelimiter = rowKeyDelimiter;
    }

    public List<String> getRowKeyColumns() {
        return rowKeyColumns;
    }

    public List<JoinTable> getJoinTables() {
        return joinTables;
    }

    public String getTableName() {

        return tableName;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public HbaseConfig getHbaseConfig() {
        return hbaseConfig;
    }

    public void setHbaseConfig(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setRowKeyColumns(List<String> rowKeyColumns) {
        this.rowKeyColumns = rowKeyColumns;
    }

    public void setJoinTables(List<JoinTable> joinTables) {
        this.joinTables = joinTables;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getJarName() {
        return jarName;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }

    public static long getCheckpointIntervar() {
        return CHECKPOINT_INTERVAR;
    }

    @Override
    public String toString() {
        String json = null;
        try {
            json = MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }

}
