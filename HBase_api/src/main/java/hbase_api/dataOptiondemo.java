package hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/1/30 - 5:59 下午
 */
public class dataOptiondemo {
    private TableName  TABLE_NAME = TableName.valueOf("WATER_BILL");

    @Test
    public void putTest() throws IOException {
        // Connection是一个重量级的对象
        // Connecton是一个线程安全的API
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.使用HBase连接获取Htable
        Table table = connection.getTable(TABLE_NAME);
        //2.构建ROWKEY、列族名、列名
        String ROWKEY = "4944191";
        String columnFamily = "C1";
        String columnName = "NAME";
        String columnADDRESS = "ADDRESS";
        String columnSEX = "SEX";
        String columnPAY_DATE = "PAY_DATE";
        String columnNUM_CURRENT = "NUM_CURRENT";
        String columnNUM_PREVIOUS = "NUM_PREVIOUS";
        String columnNUM_USAGE = "NUM_USAGE";
        String columnTOTAL_MONEY = "TOTAL_MONEY";
        String columnRECORD_DATE = "RECORD_DATE";
        String columnLATEST_DATE = "LATEST_DATE";
        //3.构建Put对象（对应put命令）
        Put put = new Put(Bytes.toBytes(ROWKEY));
        //4.添加姓名列
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes("邪见"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnADDRESS),Bytes.toBytes("东京"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnSEX),Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnPAY_DATE),Bytes.toBytes("2020-05-10"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_CURRENT),Bytes.toBytes("308.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_PREVIOUS),Bytes.toBytes("283.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_USAGE),Bytes.toBytes("25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnTOTAL_MONEY),Bytes.toBytes("150"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnRECORD_DATE),Bytes.toBytes("2020-4-25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnLATEST_DATE),Bytes.toBytes("2020-06-09"));
        //5.使用Htable表对象执行put操作
        table.put(put);
        // Htable是一个轻量级的对象，可以经常创建
        // Htable是一个非线程安全的API
        //6.关闭Htable表对象
        table.close();
        connection.close();
    }
}
