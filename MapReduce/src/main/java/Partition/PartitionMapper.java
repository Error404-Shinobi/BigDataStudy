package Partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 11:03 上午
 */
//此案例使用的输入文件为福彩开奖数据
/*
    K1：行偏移量 LongWritable
    V1：行文本数据    Text
    K2：行文本数据    Text
    V2：占位符      NullWritable

 */
public class PartitionMapper extends Mapper <LongWritable,Text,Text,NullWritable>{
    //map方法就是将K1,V1转为K2,V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}
