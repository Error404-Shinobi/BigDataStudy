package Counter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 12:13 下午
 */
public class Reducer_Counter2 extends Reducer<Text, NullWritable,Text,NullWritable> {
    public static enum Counter{
        MY_INPUT_RECOREDS,MY_INPUT_BYTES
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //枚举定义计数器
        context.getCounter(Counter.MY_INPUT_RECOREDS).increment(1L);
        context.write(key,NullWritable.get());
    }
}
