package Sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 5:35 下午
 */
public class SortReducer extends Reducer<SortBean, NullWritable,SortBean,NullWritable> {
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
