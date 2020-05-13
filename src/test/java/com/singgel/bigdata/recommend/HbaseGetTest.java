package com.singgel.bigdata.recommend;

import com.singgel.bigdata.flinksinkhbase.config.HbaseConfig;
import com.singgel.bigdata.flinksinkhbase.common.HbaseUtil;
import junit.framework.TestCase;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * \* @author singgel
 * \* @created_at: 2019/4/2 下午5:33
 * \
 */
public class HbaseGetTest extends TestCase {

    public void testHbaseGet() throws IOException {

        String user = "{\n" +
                "                \"basic:pagerank\": \"basic:pagerank\",\n" +
                "                \"basic:country\": \"basic:country\",\n" +
                "                \"basic:province\": \"basic:province\",\n" +
                "                \"basic:city\": \"basic:city\",\n" +
                "                \"basic:mobile\": \"basic:mobile\",\n" +
                "                \"basic:follower_cluster\": \"basic:follower_cluster\",\n" +
                "                \"basic:quality_cluster\": \"basic:quality_cluster\",\n" +
                "                \"basic:symbol_cluster\": \"basic:symbol_cluster\",\n" +
                "                \"basic:topic_cluster\": \"basic:topic_cluster\",\n" +
                "                \"basic:stock_click7\": \"basic:stock_click7\",\n" +
                "                \"basic:stock_show7\": \"basic:stock_show7\",\n" +
                "                \"basic:stock_click30\": \"basic:stock_click30\",\n" +
                "                \"basic:stock_show30\": \"basic:stock_show30\",\n" +
                "                \"basic:symbol_page_enter\": \"basic:symbol_page_enter\",\n" +
                "                \"basic:symbol_new_status\": \"basic:symbol_new_status\",\n" +
                "                \"basic:symbol_hot\": \"basic:symbol_hot\",\n" +
                "                \"basic:symbol_finance\": \"basic:symbol_finance\",\n" +
                "                \"basic:symbol_news\": \"basic:symbol_news\",\n" +
                "                \"basic:symbol_notice\": \"basic:symbol_notice\",\n" +
                "                \"basic:symbol_general\": \"basic:symbol_general\",\n" +
                "                \"basic:symbol_page_view\": \"basic:symbol_page_view\",\n" +
                "                \"basic:symbol_page_origin\": \"basic:symbol_page_origin\",\n" +
                "                \"basic:attention_mark\": \"basic:attention_mark\",\n" +
                "                \"basic:rebalance_num\": \"basic:rebalance_num\",\n" +
                "                \"basic:topic_personal_short_click\": \"basic:topic_personal_short_click\",\n" +
                "                \"basic:topic_personal_short_show\": \"basic:topic_personal_short_show\",\n" +
                "                \"basic:topic_personal_long_click\": \"basic:topic_personal_long_click\",\n" +
                "                \"basic:topic_personal_long_show\": \"basic:topic_personal_long_show\",\n" +
                "                \"basic:dislike_1st\": \"basic:dislike_1st\",\n" +
                "                \"basic:dislike_2st\": \"basic:dislike_2st\",\n" +
                "                \"basic:dislike_3st\": \"basic:dislike_3st\",\n" +
                "                \"basic:dislike_4st\": \"basic:dislike_4st\",\n" +
                "                \"basic:dislike_5st\": \"basic:dislike_5st\",\n" +
                "                \"basic:familar_1st\": \"basic:familar_1st\",\n" +
                "                \"basic:familar_2st\": \"basic:familar_2st\",\n" +
                "                \"basic:familar_3st\": \"basic:familar_3st\",\n" +
                "                \"basic:familar_4st\": \"basic:familar_4st\",\n" +
                "                \"basic:familar_5st\": \"basic:familar_5st\",\n" +
                "                \"basic:like_1st\": \"basic:like_1st\",\n" +
                "                \"basic:like_2st\": \"basic:like_2st\",\n" +
                "                \"basic:like_3st\": \"basic:like_3st\",\n" +
                "                \"basic:like_4st\": \"basic:like_4st\",\n" +
                "                \"basic:like_5st\": \"basic:like_5st\",\n" +
                "                \"basic:unfamilar_1st\": \"basic:unfamilar_1st\",\n" +
                "                \"basic:unfamilar_2st\": \"basic:unfamilar_2st\",\n" +
                "                \"basic:unfamilar_3st\": \"basic:unfamilar_3st\",\n" +
                "                \"basic:unfamilar_4st\": \"basic:unfamilar_4st\",\n" +
                "                \"basic:unfamilar_5st\": \"basic:unfamilar_5st\",\n" +
                "                \"basic:headline_down_cnt\": \"basic:headline_down_cnt\",\n" +
                "                \"basic:headline_up_cnt\": \"basic:headline_up_cnt\",\n" +
                "                \"basic:optional_cnt\": \"basic:optional_cnt\",\n" +
                "                \"basic:dynamic_cnt\": \"basic:dynamic_cnt\",\n" +
                "                \"basic:quotation_cnt\": \"basic:quotation_cnt\",\n" +
                "                \"basic:base_rate\": \"basic:base_rate\",\n" +
                "                \"basic:mark_gegu_enter\": \"basic:mark_gegu_enter\",\n" +
                "                \"basic:mark_share_sum\": \"basic:mark_share_sum\",\n" +
                "                \"basic:mark_head_dislike_sum\": \"basic:mark_head_dislike_sum\",\n" +
                "                \"basic:mark_status_post_user_sum\": \"basic:mark_status_post_user_sum\",\n" +
                "                \"basic:mark_search_sum\": \"basic:mark_search_sum\",\n" +
                "                \"basic:mark_debate_post_user_num\": \"basic:mark_debate_post_user_num\",\n" +
                "                \"basic:author_click_week\": \"basic:author_click_week\",\n" +
                "                \"basic:author_show_week\": \"basic:author_show_week\",\n" +
                "                \"basic:author_click_month\": \"basic:author_click_month\",\n" +
                "                \"basic:author_show_month\": \"basic:author_show_month\"\n" +
                "            }";

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = mapper.readValue(user, Map.class);

        String zookeeperQuorum = "singgel-53-3.inter.singgel.com,singgel-53-4.inter.singgel.com,singgel-53-5.inter.singgel.com,singgel-53-6.inter.singgel.com,singgel-54-3.inter.singgel.com,singgel-54-4.inter.singgel.com,singgel-54-5.inter.singgel.com,singgel-54-6.inter.singgel.com";
        HbaseConfig hbaseConfig = new HbaseConfig(zookeeperQuorum, "2181", "/hbase-unsecure", 1, 0L, new HashMap<>());

        HbaseUtil hbaseUtil = new HbaseUtil(hbaseConfig);
        String[] uids = {"3148682933", "3188053557", "2912663770", "3227054543", "1492133910", "1275730031"};
        String[] statusIds = {"124616652","124650145","124458448","124628342","124386412","124382379","124303730","124479145","124580988","124331284"};

//        long start1 = System.currentTimeMillis();
//        for (String uid : uids) {
//            Get get = new Get(Bytes.toBytes(uid));
//            map.keySet().forEach(e -> {
//                get.addFamily(Bytes.toBytes(e.split(":")[0]));
//            });
//            hbaseUtil.singleGet("user_feature", get);
//        }
//        long start2 = System.currentTimeMillis();
//        for (String uid : uids) {
//            Get get = new Get(Bytes.toBytes(uid));
//            map.keySet().forEach(e -> {
//                get.addColumn(Bytes.toBytes(e.split(":")[0]), Bytes.toBytes(e.split(":")[1]));
//            });
//            hbaseUtil.singleGet("user_feature", get);
//        }
        long start3 = System.currentTimeMillis();
//        for (String uid : uids) {
//            hbaseUtil.singleGet("user_feature", new Get(Bytes.toBytes(uid)));
//        }

        for (String sid : statusIds) {
            hbaseUtil.singleGet("status_feature_string", new Get(Bytes.toBytes(sid)));
        }

        long start4 = System.currentTimeMillis();

//        System.out.println(String.format("get with given families cost: %d ms", start2 - start1));
//        System.out.println(String.format("get with given columns cost: %d ms", start3 - start2));
        System.out.println(String.format("get with no given columns cost: %d ms", start4 - start3));

    }
}
