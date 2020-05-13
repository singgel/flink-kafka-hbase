package com.singgel.bigdata.recommend;

import com.singgel.bigdata.flinksinkhbase.common.HbaseUtil;
import com.singgel.bigdata.flinksinkhbase.common.JoinTable;
import com.singgel.bigdata.flinksinkhbase.common.ValueFormat;
import com.singgel.bigdata.flinksinkhbase.config.HbaseConfig;
import com.singgel.bigdata.flinksinkhbase.config.JobConfig;
import com.singgel.bigdata.flinksinkhbase.config.KafkaConfig;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/25 上午11:40
 * \
 */
public class JobConfigTest extends TestCase {

    @Test
    public void testJobConfig() throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        String input = "3521088992|1553845345473_1420|16|USER_NEW_STATUS|3|124029277|1|33|3|25|2|0|0|0.0|0.0|200.0|0.0|0.0|0.0|-1|0.0||223.104.4.109|移动|中国||0.0|timeline_status_tf_rerank_v1|1049|0.0|5747931945392156544|0.3354492783546448||4487|19264|0|0|0|0|3|#|EWC=3-GOOGL=1-RUSS=1-JEQ=1-ENZL=1-BA=1|n,n,0,0,359.0,334.0||330.61522521972654|47=20-42=19-55=14-24=5-58=5-62=4-28=2-64=2-66=1-57=1-25=1-48=1-61=1#1109218013=3-8806901933=3-9699237903=2-9383578508=2-7807472003=2-9725962662=2-1978838521=2-1558983019=2-9139068670=2-3826938159=2-5124430882=2-1077346177=1-1683324140=1-1383227981=1-3216693756=1-9688940470=1-1697559028=1-1703783922=1-6028781397=1-1250983240=1#=4-SZ002430=2-EWC=1-HUAW=1-EWY=1-SZ300436=1-BA=1#94_3=10-54_3=6-319_3=5-80_3=5-95_3=4-182_3=4-12_3=3-343_3=2-308_3=2-218_3=2-64_3=2-337_3=2-224_3=2-260_3=2-139_3=1-116_3=1-137_3=1-76_3=1-157_3=1-78_3=1#中国=3-飞机=2-成本=2-总理=2-合作=2-市场=2-技>术=2-季报=2-板块=2-状态=1-工业股=1-中美=1-金叉=1-牛市=1-妈妈=1-谈判=1-爸爸=1-新西兰=1-上证指数=1-工作=1|高手|";

        Map<String, String> columnsConfig = new HashMap<>();
        columnsConfig.put("0", "basic:time");
        columnsConfig.put("1", "basic:userId");
        columnsConfig.put("2", "basic:sessionId");
        columnsConfig.put("3", "basic:testMissionId");
        columnsConfig.put("4", "basic:strategeName");
        columnsConfig.put("5", "basic:statusType");
        columnsConfig.put("6", "basic:statusId");
        columnsConfig.put("7", "basic:position");
        columnsConfig.put("8", "basic:likeCount");
        columnsConfig.put("9", "basic:retweetCount");
        columnsConfig.put("10", "basic:replyCount");
        columnsConfig.put("20", "basic:tag");
        columnsConfig.put("22", "basic:stockSymbol");
        columnsConfig.put("31", "basic:randomId");
        columnsConfig.put("33", "basic:quoteString");
        columnsConfig.put("41", "basic:contextInfo");


        Map<String, String> userFeatureColumnMapping = new HashMap<>();
        userFeatureColumnMapping.put("basic:pagerank", "basic:pagerank");
        userFeatureColumnMapping.put("basic:country", "basic:country");
        userFeatureColumnMapping.put("basic:province", "basic:province");
        userFeatureColumnMapping.put("basic:city", "basic:city");
        userFeatureColumnMapping.put("basic:mobile", "basic:mobile");
        userFeatureColumnMapping.put("basic:follow_cluster", "basic:follow_cluster");
        userFeatureColumnMapping.put("basic:quality_cluster", "basic:quality_cluster");
        userFeatureColumnMapping.put("basic:symbol_cluster", "basic:symbol_cluster");

        JoinTable userFeature = new JoinTable("user_feature", "basic:userId", userFeatureColumnMapping);

        Map<String, String> statusFeatureColumnMapping = new HashMap<>();

        statusFeatureColumnMapping.put("basic:user_id", "basic:user_id");
        statusFeatureColumnMapping.put("basic:symbol_id", "basic:symbol_id");
        statusFeatureColumnMapping.put("basic:created_at", "basic:created_at");
        statusFeatureColumnMapping.put("basic:source", "basic:source");
        statusFeatureColumnMapping.put("basic:retweet_status_id", "basic:retweet_status_id");
        statusFeatureColumnMapping.put("basic:paid_mention_user_id", "basic:paid_mention_user_id");
        statusFeatureColumnMapping.put("basic:retweet_user_id", "basic:retweet_user_id");
        statusFeatureColumnMapping.put("basic:retweet_symbol_id", "basic:retweet_symbol_id");
        statusFeatureColumnMapping.put("basic:truncated", "basic:truncated");
        statusFeatureColumnMapping.put("basic:flags", "basic:flags");
        statusFeatureColumnMapping.put("basic:expired_at", "basic:expired_at");
        statusFeatureColumnMapping.put("basic:title_length", "basic:title_length");
        statusFeatureColumnMapping.put("basic:title_hash", "basic:title_hash");

        JoinTable statusFeature = new JoinTable("status_feature_string", "basic:statusId", statusFeatureColumnMapping);

        List<JoinTable> joinTables = new ArrayList<>();
        joinTables.add(userFeature);
        joinTables.add(statusFeature);

        String bootstrtapServers = "localhost:9092";
        String topic = "recommend2.statistics";
        String groupId = "flink_recommend2_statistic_test";
        ValueFormat valueFormat = ValueFormat.CSV;
        String delimiter = "|";
        KafkaConfig kafkaConfig = new KafkaConfig(bootstrtapServers, topic, groupId, valueFormat, delimiter, new HashMap<>());
        String zookeeperQuorum = "singgel-53-3.inter.singgel.com,singgel-53-4.inter.singgel.com,singgel-53-5.inter.singgel.com,singgel-53-6.inter.singgel.com,singgel-54-3.inter.singgel.com,singgel-54-4.inter.singgel.com,singgel-54-5.inter.singgel.com,singgel-54-6.inter.singgel.com";
        HbaseConfig hbaseConfig = new HbaseConfig(zookeeperQuorum, "2181", "/hbase-unsecure", 1, 0L, new HashMap<>());

        String rowKeyDelimiter = "#";
        List<String> rowKeyColumns = new ArrayList<>();
        rowKeyColumns.add("basic:userId");
        rowKeyColumns.add("basic:statusId");
        String tableName = "test";
        String jobName = "recommend_feature_hbase";
        JobConfig jobConfig = new JobConfig(columnsConfig, rowKeyDelimiter, rowKeyColumns, tableName, kafkaConfig, hbaseConfig, joinTables, jobName, 2, "");

        String jobConfigJosn = jobConfig.toString();

        String expected = "{\"indexColumnMapping\":{\"22\":\"basic:stockSymbol\",\"33\":\"basic:quoteString\",\"0\":\"basic:time\",\"1\":\"basic:userId\",\"2\":\"basic:sessionId\",\"3\":\"basic:testMissionId\",\"4\":\"basic:strategeName\",\"5\":\"basic:statusType\",\"6\":\"basic:statusId\",\"7\":\"basic:position\",\"8\":\"basic:likeCount\",\"9\":\"basic:retweetCount\",\"41\":\"basic:contextInfo\",\"20\":\"basic:tag\",\"31\":\"basic:randomId\",\"10\":\"basic:replyCount\"},\"rowKeyDelimiter\":\"#\",\"rowKeyColumns\":[\"basic:userId\",\"basic:statusId\"],\"tableName\":\"test\",\"kafkaConfig\":{\"bootstrapServers\":\"localhost:9092\",\"topic\":\"recommend2.statistics\",\"groupId\":\"flink_recommend2_statistic_test\",\"valueFormat\":\"CSV\",\"delimiter\":\"|\",\"optionalProps\":{}},\"hbaseConfig\":{\"zookerperQuorum\":\"singgel-53-3.inter.singgel.com,singgel-53-4.inter.singgel.com,singgel-53-5.inter.singgel.com,singgel-53-6.inter.singgel.com,singgel-54-3.inter.singgel.com,singgel-54-4.inter.singgel.com,singgel-54-5.inter.singgel.com,singgel-54-6.inter.singgel.com\",\"port\":\"2181\",\"zookeeperZondeParent\":\"/hbase-unsecure\",\"optionalProp\":{},\"batchCount\":1,\"interval\":0},\"jobName\":\"recommend_feature_hbase\",\"parallelism\":2,\"jarName\":\"\",\"joinTables\":[{\"tableName\":\"user_feature\",\"joinKey\":\"basic:userId\",\"columnsMapping\":{\"basic:pagerank\":\"basic:pagerank\",\"basic:city\":\"basic:city\",\"basic:symbol_cluster\":\"basic:symbol_cluster\",\"basic:country\":\"basic:country\",\"basic:follow_cluster\":\"basic:follow_cluster\",\"basic:quality_cluster\":\"basic:quality_cluster\",\"basic:mobile\":\"basic:mobile\",\"basic:province\":\"basic:province\"}},{\"tableName\":\"status_feature_string\",\"joinKey\":\"basic:statusId\",\"columnsMapping\":{\"basic:source\":\"basic:source\",\"basic:title_hash\":\"basic:title_hash\",\"basic:symbol_id\":\"basic:symbol_id\",\"basic:retweet_user_id\":\"basic:retweet_user_id\",\"basic:user_id\":\"basic:user_id\",\"basic:title_length\":\"basic:title_length\",\"basic:retweet_status_id\":\"basic:retweet_status_id\",\"basic:flags\":\"basic:flags\",\"basic:paid_mention_user_id\":\"basic:paid_mention_user_id\",\"basic:created_at\":\"basic:created_at\",\"basic:retweet_symbol_id\":\"basic:retweet_symbol_id\",\"basic:expired_at\":\"basic:expired_at\",\"basic:truncated\":\"basic:truncated\"}}]}";


        Assert.assertEquals(expected, jobConfigJosn);

        JobConfig reJobConfig = mapper.readValue(jobConfig.toString(), JobConfig.class);
        reJobConfig.validate();
        HbaseUtil hbaseUtil = new HbaseUtil(reJobConfig.getHbaseConfig());
        hbaseUtil.prepareTable(reJobConfig.getTableName(), reJobConfig.families());

        Assert.assertEquals("#", reJobConfig.getRowKeyDelimiter());
        Assert.assertEquals("[basic]", reJobConfig.families().toString());

        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("key", "2019-03-27 09:23:00");
        node.put("value", input);

    }

}
