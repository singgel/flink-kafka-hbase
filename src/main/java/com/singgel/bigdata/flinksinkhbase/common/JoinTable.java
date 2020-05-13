package com.singgel.bigdata.flinksinkhbase.common;

import java.io.Serializable;
import java.util.Map;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午5:03
 * \
 */

/**
 * 需要join的Hbase表，按照主表中的某列值作为此表中的rowKey,获取此表此rowKey的相关列值，插入到主表对应的列，完成join
 */
public class JoinTable implements Serializable{

    /**
     * hbase 表名
     */
    private String tableName;

    /**
     * 和此表RowKey相关联的主表列名
     */
    private String joinKey;

    /**
     * 此表中列簇和列的对应关系
     */

    /**
     * 此表中列和列簇及写入到主表中的列簇的对应关系,如：
     * key-> fromFamily:fromColumn
     * value -> toFamily:toColumn
     */
    private Map<String, String> columnsMapping;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public Map<String, String> getColumnsMapping() {
        return columnsMapping;
    }

    public void setColumnsMapping(Map<String, String> columnsMapping) {
        this.columnsMapping = columnsMapping;
    }

    public JoinTable() {
    }

    public JoinTable(String tableName, String joinKey, Map<String, String> columnsMapping) {
        this.tableName = tableName;
        this.joinKey = joinKey;
        this.columnsMapping = columnsMapping;
    }
}
