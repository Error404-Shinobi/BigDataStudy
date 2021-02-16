package Partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 11:17 上午
 */

/*
    K2：行文本数据    Text
    V2：占位符      NullWritable
    K3：行文本数据    Text
    V3：占位符      NullWritable
 */
public class PartitionerReducer extends Reducer <Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
