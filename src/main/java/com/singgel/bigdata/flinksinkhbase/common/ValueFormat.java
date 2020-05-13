package com.singgel.bigdata.flinksinkhbase.common;

/**
 * kafka消息值的类型格式
 */
public enum ValueFormat {
    /**
     * CSV格式，以固定分隔符分割
     */
    CSV,

    /**
     * ObjecNode的json格式
     */
    JSON

}
