package Combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 9:18 下午
 */
public class MyCombiner extends Reducer<Text, LongWritable,Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        LongWritable longWritable = new LongWritable(count);
        //1：遍历集合，将集合中的数字相加，得到V3
        for (LongWritable value : values) {
            count += value.get();
        }
        //2：将K3和V3写入上下文中
        context.write(key,new LongWritable(count));
    }
}
