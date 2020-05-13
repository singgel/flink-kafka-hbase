package com.singgel.bigdata.flinksinkhbase;

import com.singgel.bigdata.flinksinkhbase.common.CustomJsonDeserializationSchema;
import com.singgel.bigdata.flinksinkhbase.common.HbaseUtil;
import com.singgel.bigdata.flinksinkhbase.common.JobConfigManager;
import com.singgel.bigdata.flinksinkhbase.config.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.client.Put;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午1:49
 * \
 */
@Slf4j
public class FlinkRunner {

    public static void main(String[] args) throws Exception {

        String jobKey;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            jobKey = params.get("jobKey");
        } catch (Exception e) {
            System.err.println("No jobKey specified. Please run 'FlinkRunner --jobKey <jobKey>'");
            return;
        }

        JobConfig jobConfig = JobConfigManager.getConfigByKey(jobKey);
        jobConfig.validate();

        HbaseUtil hbaseUtil = new HbaseUtil(jobConfig.getHbaseConfig());
        hbaseUtil.prepareTable(jobConfig.getTableName(), jobConfig.families());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().registerKryoType(JobConfig.class);
        env.getConfig().registerKryoType(Put.class);
        env.getConfig().registerKryoType(HbaseUtil.class);

        Properties prop = jobConfig.getKafkaConfig().kafkaProps();
        DataStreamSource<ObjectNode> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(jobConfig.getKafkaConfig().getTopic(),
                new CustomJsonDeserializationSchema(true), prop));

        dataStreamSource.addSink(new HbaseSink(jobConfig));

        //设置最大失败重启尝试次数及每次重启间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10L, TimeUnit.SECONDS)
        ));

        env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAR, CheckpointingMode.EXACTLY_ONCE);

        env.execute(jobConfig.getJobName());
    }

}
