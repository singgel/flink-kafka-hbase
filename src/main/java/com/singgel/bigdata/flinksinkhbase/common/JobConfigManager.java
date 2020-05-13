package com.singgel.bigdata.flinksinkhbase.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import com.singgel.bigdata.flinksinkhbase.config.JobConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午5:38
 * \
 */
public class JobConfigManager {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String URL = ConfigFactory.load().getConfig("apollo").getString("url");

    /**
     * 从Apollo配置中心获取jobConfig的配置
     *
     * @param key 配置的key
     * @return JobConfig
     * @throws IOException
     */
    public static JobConfig getConfigByKey(String key) throws Exception {

        java.net.URL url = new URL(String.format("%s/apollo/getConf?key=%s",URL, key));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URLSTR : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //打印结果
        System.out.println(response.toString());
        String jsonRet = MAPPER.readTree(response.toString()).get("data").asText();
        JobConfig jobConfig = MAPPER.readValue(jsonRet, JobConfig.class);

        return jobConfig;

    }
}
