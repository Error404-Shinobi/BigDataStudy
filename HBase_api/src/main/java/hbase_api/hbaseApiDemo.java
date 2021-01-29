package hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
//import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author xuansama
 * @date 2021/1/27 - 5:51 下午
 */
public class hbaseApiDemo {
    /*
    创建Hbase连接以及admin对象
     */
    private Connection connection;
    private Admin admin;
    @Test
    public void beforeTest() throws IOException {
        //1.使用HbaseConfiguration.create()创建HBase配置
        Configuration configuration = HBaseConfiguration.create();
        //2.使用ConnectionFactory.createConnection()创建HBase连接
        connection = ConnectionFactory.createConnection(configuration);
        //3.要创建表，需要基于HBase连接获取admin管理对象
        //要创建、删除表需要和HMaster连接，所以需要有一个admin对象
        admin = connection.getAdmin();
    }

    @Test
    public void createTableTest() throws IOException {
        //tableExists()需要的一个参数为TableName，所以要创建一个表名
        TableName tableName = TableName.valueOf("WATER_BILL");
        //1.判断表是否存在
        if (admin.tableExists(tableName)){
            //存在，则退出
            return;
        }
        //不存在，则构建
        //2.使用TableDescriptorBuilder.newBuilder()构建表描述构建器
        //3.TableDescriptor：表描述器，描述表有多少个列族及其他的属性
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        //3.使用ColumnFamilyDescriptorBuilder.newBuilder()构建列族描述器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C1"));
        //使用一个工具类：Bytes（HBase包下的Bytes工具类），可以将字符串、long、double类型转换成byte[]数组
        //也可以将byte[]数组转换为指定类型

        //4.构建列族描述，构建表描述
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        //建立表和列族的关联
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //5.创建表
        admin.createTable(tableDescriptor);
    }

    @Test
    public void deleteTableTest() throws IOException {
        ZooKeeper zkClient = new ZooKeeper("172.16.74.100:2181", 3000, new Watcher(){
            @Override
            public void process(WatchedEvent event){

            }
        });
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //1.判断表是否存在
        TableName tableName = TableName.valueOf("WATER_BILL");
        if (admin.tableExists(tableName)){
            //2.如果存在，则禁用表
            admin.disableTable(tableName);
            //3.再删除表
            admin.deleteTable(tableName);
        }
    }
    @Test
    public void afterTest() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //4.使用admin.close(),connection.close()关闭连接
        admin.close();
        connection.close();
    }
}
