package Reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/25 - 10:04 上午
 */
public class JoinMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    }
}
