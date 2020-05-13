# flink-kafka-hbase
功能：实现kafka消息实时落地hbase，支持csv/json字符串两种格式的消息，支持自定义组合rowkey,列簇和列名，支持按照kafka消息流中不同字段join不同的hbase表，并自定义写入列簇和列(join时需评估一下性能)  
支持at least once语义  
外部依赖：apollo配置中心，本项目依靠配置驱动，配置存储在apollo配置中心  
配置：
```
{
    "indexColumnMapping": {    --indexColumnMapping即CSV格式消息的key和value按照value里的分隔符拼接后再分割后下标及写入hbase列的对应关系
        "0": "basic:time",     --第0列始终是kafka消息的key，如果不需要可以不指定
        "1": "basic:user_id",
        "2": "basic:session_id",
        "3": "basic:test_mission_id",
        "4": "basic:stratege_name",
        "5": "basic:status_type",
        "6": "basic:status_id",
        "7": "basic:position",
        "8": "basic:like_count",
        "9": "basic:retweet_count",
        "10": "basic:reply_count",
        "11": "basic:fav_count",
        "12": "basic:reward_amount",
        "13": "basic:reward_user_count",
        "14": "basic:status_hot_score",
        "15": "basic:status_hot_score_norm",
        "16": "basic:user_score",
        "17": "basic:use_score_norm",
        "18": "basic:stock_score",
        "19": "basic:stock_score_norm",
        "20": "basic:tag",
        "21": "basic:tag_score",
        "22": "basic:stock_symbol",
        "23": "basic:ip",
        "24": "basic:device",
        "25": "basic:country_name",
        "26": "basic:city_name",
        "27": "basic:topic_score",
        "28": "basic:rerank_name",
        "29": "basic:author_block_count",
        "30": "basic:percent",
        "31": "basic:random_id",
        "32": "basic:rank_score",
        "33": "basic:quote_string",
        "34": "basic:click_num",
        "35": "basic:show_num",
        "36": "basic:tag_short_term_click",
        "37": "basic:tag_short_term_show",
        "38": "basic:tag_long_term_click",
        "39": "basic:tag_long_term_show",
        "40": "basic:block_count",
        "41": "basic:context_info",
        "42": "basic:recent_behavior",
        "43": "basic:basic_string",
        "44": "basic:mention_stock_rank",
        "45": "basic:text_quality_score",
        "46": "basic:last_nc_context",
        "47": "basic:keywords"
    },
    "rowKeyDelimiter": "#",    --如果rowkey是多个列的拼接，则需指定的拼接符
    "rowKeyColumns": [         --rowkey组成的列
        "basic:user_id",
        "basic:statusId"
    ],
    "tableName": "cy_test",    --数据流要写入hbase表的表名（如不存在会自动创建）
    "kafkaConfig": {           --flink接入kafka数据源的配置
        "bootstrapServers": "singgel:9092",    --kafka的broker list
        "topic": "recommend2.statistics",         --需要接入的topic
        "groupId": "flink_recommend2_statistic_join_test2",    --flink中消费kafka topic的groupId
        "delimiter": "|",          --kafka消息value的分隔符，当valueFormat=CSV时必须指定       
        "valueFormat": "CSV",      --kafka消息value的格式，目前支持"CSV"和"JSON"两种
        "optionalProps": {}        --其他kafka消费者配置
    },
    "hbaseConfig": {      --写入的hbase集群配置
        "zookerperQuorum": "singgel-53-3.inter.singgel.com,singgel-53-4.inter.singgel.com,singgel-53-5.inter.singgel.com,singgel-53-6.inter.singgel.com,singgel-54-3.inter.singgel.com,singgel-54-4.inter.singgel.com,singgel-54-5.inter.singgel.com,singgel-54-6.inter.singgel.com",
        "port": "2181",
        "zookeeperZondeParent": "/hbase-unsecure",      --hbase在zookeeper中的根目录节点名称（注意：咱内部cdh集群是/hbase，此处是ambari集群hbase的配置）
        "batchCount": 100,           --批量写入的条数，与interval条件满足其一就触发写入，注：当接入的topic数据源生产速率较小时且无join时，可以设置为1，逐条写入
        "interval": 5000,            --批量写入的间隔时间
        "optionalProp": {}           --其他hbase设置
    },
    "jobName": "recommend_feature_hbase_sink_test",    --flink job的名称
    "parallelism": 8,               --flink任务执行时的平行度
    "jarName": "flink-kafka-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar",    --执行flink任务的jar包，当通过实时平台界面提交flinkjob时需要指定
    "joinTables": [  --需要join的表，可以指定多个，可以为空；当要join多个表时，需要评估一下性能
        {
            "tableName": "user_feature",     --需要join的hbase表
            "joinKey": "basic:userId",       --join的字段，需要在"indexColumnMapping的values中，且是joinTable的rowKey
            "columnsMapping": {       --join表中的列和要写入表中的列的对应关系，key->fromFamily:fromColumn，value->toFamily:toColumn，from和to的列簇和列不需一致
                "basic:pagerank": "basic :pagerank",     
                "basic:country": "basic:country",
                "basic:province": "basic:province",
                "basic:city": "basic:city",
                "basic:mobile": "basic:mobile",
                "basic:follower_cluster": "basic:follower_cluster",
                "basic:quality_cluster": "basic:quality_cluster",
                "basic:symbol_cluster": "basic:symbol_cluster",
                "basic:topic_cluster": "basic:topic_cluster",
                "basic:stock_click7": "basic:stock_click7",
                "basic:stock_show7": "basic:stock_show7",
                "basic:stock_click30": "basic:stock_click30",
                "basic:stock_show30": "basic:stock_show30",
                "basic:symbol_page_enter": "basic:symbol_page_enter",
                "basic:symbol_new_status": "basic:symbol_new_status",
                "basic:symbol_hot": "basic:symbol_hot",
                "basic:symbol_finance": "basic:symbol_finance",
                "basic:symbol_news": "basic:symbol_news",
                "basic:symbol_notice": "basic:symbol_notice",
                "basic:symbol_general": "basic:symbol_general",
                "basic:symbol_page_view": "basic:symbol_page_view",
                "basic:symbol_page_origin": "basic:symbol_page_origin",
                "basic:attention_mark": "basic:attention_mark",
                "basic:rebalance_num": "basic:rebalance_num",
                "basic:topic_personal_short_click": "basic:topic_personal_short_click",
                "basic:topic_personal_short_show": "basic:topic_personal_short_show",
                "basic:topic_personal_long_click": "basic:topic_personal_long_click",
                "basic:topic_personal_long_show": "basic:topic_personal_long_show",
                "basic:dislike_1st": "basic:dislike_1st",
                "basic:dislike_2st": "basic:dislike_2st",
                "basic:dislike_3st": "basic:dislike_3st",
                "basic:dislike_4st": "basic:dislike_4st",
                "basic:dislike_5st": "basic:dislike_5st",
                "basic:familar_1st": "basic:familar_1st",
                "basic:familar_2st": "basic:familar_2st",
                "basic:familar_3st": "basic:familar_3st",
                "basic:familar_4st": "basic:familar_4st",
                "basic:familar_5st": "basic:familar_5st",
                "basic:like_1st": "basic:like_1st",
                "basic:like_2st": "basic:like_2st",
                "basic:like_3st": "basic:like_3st",
                "basic:like_4st": "basic:like_4st",
                "basic:like_5st": "basic:like_5st",
                "basic:unfamilar_1st": "basic:unfamilar_1st",
                "basic:unfamilar_2st": "basic:unfamilar_2st",
                "basic:unfamilar_3st": "basic:unfamilar_3st",
                "basic:unfamilar_4st": "basic:unfamilar_4st",
                "basic:unfamilar_5st": "basic:unfamilar_5st",
                "basic:headline_down_cnt": "basic:headline_down_cnt",
                "basic:headline_up_cnt": "basic:headline_up_cnt",
                "basic:optional_cnt": "basic:optional_cnt",
                "basic:dynamic_cnt": "basic:dynamic_cnt",
                "basic:quotation_cnt": "basic:quotation_cnt",
                "basic:base_rate": "basic:base_rate",
                "basic:mark_gegu_enter": "basic:mark_gegu_enter",
                "basic:mark_share_sum": "basic:mark_share_sum",
                "basic:mark_head_dislike_sum": "basic:mark_head_dislike_sum",
                "basic:mark_status_post_user_sum": "basic:mark_status_post_user_sum",
                "basic:mark_search_sum": "basic:mark_search_sum",
                "basic:mark_debate_post_user_num": "basic:mark_debate_post_user_num",
                "basic:author_click_week": "basic:author_click_week",
                "basic:author_show_week": "basic:author_show_week",
                "basic:author_click_month": "basic:author_click_month",
                "basic:author_show_month": "basic:author_show_month"
            }
        },
        {
            "tableName": "status_feature_string",
            "joinKey": "basic:statusId",
            "columnsMapping": {
                "basic:user_id": "basic:user_id",
                "basic:symbol_id": "basic:symbol_id",
                "basic:created_at": "basic:created_at",
                "basic:source": "basic:source",
                "basic:retweet_status_id": "basic:retweet_status_id",
                "basic:paid_mention_id": "basic:paid_mention_id",
                "basic:retweet_user_id": "basic:retweet_user_id",
                "basic:retweet_symbol_id": "basic:retweet_symbol_id",
                "basic:truncate": "basic:truncate",
                "basic:flags": "basic:flags",
                "basic:expired_at": "basic:expired_at",
                "basic:title_length": "basic:title_length",
                "basic:title_hash": "basic:title_hash",
                "basic:title_flag": "basic:title_flag",
                "basic:text_length": "basic:text_length",
                "basic:pic_count": "basic:pic_count",
                "basic:type": "basic:type",
                "basic:meta_classes": "basic:meta_classes",
                "basic:pic_score": "basic:pic_score",
                "basic:domain": "basic:domain",
                "basic:url_hash": "basic:url_hash",
                "basic:character_percent": "basic:character_percent",
                "basic:symbol": "basic:symbol",
                "basic:keyword": "basic:keyword",
                "basic:match_word": "basic:match_word",
                "basic:keyword_title": "basic:keyword_title",
                "basic:keyword_des": "basic:keyword_des",
                "basic:symbol_title": "basic:symbol_title",
                "basic:symbol_sim_title": "basic:symbol_sim_title",
                "basic:symbol_des": "basic:symbol_des",
                "basic:symbol_sim_des": "basic:symbol_sim_des",
                "basic:symbol_content": "basic:symbol_content",
                "basic:symbol_sim_content": "basic:symbol_sim_content"
            }
        }
    ]
}
```

在flink任务启动时，会去apollo配置中心取指定的配置，根据配置执行任务。

关键实现HbaseSink代码如下：
```
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
        checkpointedState.clear();
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
```

开发过程中遇到的问题主要有两点，一是处理速度，二是批量处理时出错数据丢失的问题。

对于处理速度的优化：

（1）由最开始的单条写入改成批量写入，但在获取joinTable的列时依然是逐条获取，每个rowkey调用一次get方法，比较费时

（2）将joinTable的逐条get，改成批量get，速度提升了4-5倍，一是因为减少了提交请求的次数，加快返回速度；二是因为短时间内recommend2.statistics记录user_id和status_id分别都有重复，批量时可以减少实际查询rowkey的个数进而节省时间。

（3）尽管做了以上两点优化，但速度还是很慢，经过打日志发现，主要是user_feature这个表获取rowkey的值太慢，于是又在发送get请求时做了如下优化： 

因为user_feature的所有列都插入到集成的表中，一开始就没有在get请求时指定要获取的列簇和列名，优化就是在提交get请求时，指定所有需要获取的列簇和列名，这样明显快很多，大概提升10倍，此时写入一条join后的数据大概耗时2-3ms
| batch_get user_feature(139) 134 ms| batch_get status_feature_string(160) 59 ms | batch_put(200) cost 195 ms | batch_total(200) cost 433 ms | per record cost 2 ms
| batch_get user_feature(134) 132 ms| batch_get status_feature_string(169) 56 ms | batch_put(200) cost 201 ms | batch_total(200) cost 434 ms | per record cost 2 ms 

对于处理出错数据丢失的问题：

数据丢失的场景：(1)当HbaseSink中调用了多次invoke方法，nodes中累积了一定的数量，但还没有触发写入操作，此时flink程序由于某种原因失败了自动重启，之前nodes中累积的记录就会丢失。

怎样做到数据不丢失？

（1）让HbaseSink实现CheckpointedFunction接口，实现snapshotState和initializeState方法，snapshotState的调用频率和FlinkRunner中指定的checkpoint的频率一致，每次checkpoint会提交kafka的offset，并执行snapshotState方法，在snapshotState方法中，会将nodes中的元素加入到checkpointState中，当flink程序失败自动重启后，initializeState方法会从checkpointState中恢复nodes中的数据，接着处理。

（2）当flink任务重试几次失败导致任务最终失败或者手动停止flink任务，再重新提交flink任务时，checkpointedState会是新的对象，不会保存上次任务失败或停止时nodes中的数据，这种情况依然会丢数据，因为程序失败自动重启和手动停止时都会调用close方法，因此在close方法中调用batchFlush方法，先写入再关闭。但重新启动时，从上次checkpoint到停止时的消息会重复处理。
