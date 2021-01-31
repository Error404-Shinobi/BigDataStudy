package hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @Author xuansama
 * @Date 2021/1/30 - 5:59 下午
 */
public class dataOptiondemo {
    private TableName TABLE_NAME = TableName.valueOf("WATER_BILL");

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
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnADDRESS), Bytes.toBytes("东京"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnSEX), Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnPAY_DATE), Bytes.toBytes("2020-05-10"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_CURRENT), Bytes.toBytes("308.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_PREVIOUS), Bytes.toBytes("283.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNUM_USAGE), Bytes.toBytes("25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnTOTAL_MONEY), Bytes.toBytes("150"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnRECORD_DATE), Bytes.toBytes("2020-4-25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnLATEST_DATE), Bytes.toBytes("2020-06-09"));
        //5.使用Htable表对象执行put操作
        table.put(put);
        // Htable是一个轻量级的对象，可以经常创建
        // Htable是一个非线程安全的API
        //6.关闭Htable表对象
        table.close();
        connection.close();
    }

    @Test
    public void getTest() throws IOException {
        //1.获取HTable
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TABLE_NAME);
        //2.使用ROWKEY构建Get对象
        Get get = new Get(Bytes.toBytes("4944191"));
        //3.执行get请求
        Result result = table.get(get);
        //4.获取所有单元格
        List<Cell> cellList = result.listCells();
        //5.打印ROWKEY
        byte[] rowkey = result.getRow();
        System.out.println(Bytes.toString(rowkey));
        //6.迭代单元格列表
        for (Cell cell : cellList) {
            // 将字节数组转换为字符串
            // 获取列族名称
            String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            // 获取列名称
            String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            // 获取值
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(columnFamily + ":" + columnName + "->" + value);
        }
        //7.关闭表
        table.close();
        connection.close();
    }

    @Test
    public void deleteTest() throws IOException {
        // 1.获取HTable对象
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TABLE_NAME);
        // 2.根据ROWKEY构建delete对象
        Delete delete = new Delete(Bytes.toBytes("4944191"));
        // 3.执行delete请求
        table.delete(delete);
        // 4.关闭表
        table.close();
        connection.close();
    }

    @Test
    public void scanFilterTest() throws IOException {
        // 1.获取表
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TABLE_NAME);
        // 2.构建scan请求对象
        Scan scan = new Scan();
        // 3.构建两个过滤器
        // a)构建两个日期范围过滤器（此处选择RECORD_DATE
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"), Bytes.toBytes("RECORD_DATE"), CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("2020-06-01")));
        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"), Bytes.toBytes("RECORD_DATE"), CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("2020-06-30")));
        // b)构建过滤器列表
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, startFilter, endFilter);
        // 4.执行scan扫描请求
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        // 5.迭代打印result
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            List<Cell> cellList = result.listCells();
            byte[] rowkey = result.getRow();
            System.out.println(Bytes.toString(rowkey));
            //6.迭代单元格列表
            for (Cell cell : cellList) {
                // 将字节数组转换为字符串
                // 获取列族名称
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                // 获取列名称
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                // 获取值
                String value = "";
                // 解决乱码问题：
                // 思路：
                // 如果某个列是以下列中的其中一个，调用toDouble让程序认为它是一个数值来转换
                //NUM_CURRENT、NUM_PREVIOUS、NUM_USAGE、TOTAL_MONEY
                if (columnName.equals("NUM_CURRENT")
                        || columnName.equals("NUM_PREVIOUS")
                        || columnName.equals("NUM_USAGE")
                        || columnName.equals("TOTAL_MONEY")){
                        value = Bytes.toDouble(cell.getValueArray()) + "";
                }
                else {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
                System.out.println(columnFamily + ":" + columnName + "->" + value);
            }
            // 7.关闭ResultScanner(这个操作非常占用资源，所以使用后一定要关闭)
            resultScanner.close();
            // 8.关闭表
            table.close();
            connection.close();
        }
    }
}
