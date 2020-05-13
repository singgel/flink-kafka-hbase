package com.singgel.bigdata.recommend;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/28 上午10:37
 * \
 */
public class HbaseByteTest extends TestCase{

    public void testByte(){
        String userIdStr = "124235435235.4";
        Long userIdLong = 124235435235L;


        byte[] strByte = Bytes.toBytes(userIdStr);
        byte[] longByte = Bytes.toBytes(userIdLong);

        System.out.println(Bytes.toString(strByte));

        //long型写入的，读成string型，会乱码
        System.out.println(Bytes.toString(longByte));

        //string型写入的，读成long型，结果不对
        System.out.println(Bytes.toLong(strByte));

    }
}
