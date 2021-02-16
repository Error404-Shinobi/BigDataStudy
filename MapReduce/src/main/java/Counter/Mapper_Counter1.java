package Counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 12:03 下午
 */
public class Mapper_Counter1 extends Mapper <LongWritable,Text,Text,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //MR_COUNTER为自定义计数器类型的名称，partition_counter为自定义计数器输出变量的名称
        Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        //每次执行该方法则计数器变量的值加一（变量为Long类型）
        counter.increment(1L);
        context.write(value,NullWritable.get());
    }
}
