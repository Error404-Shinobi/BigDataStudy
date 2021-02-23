package Flow_Sort_demo2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/23 - 2:51 下午
 */
public class FlowSortReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    //reduce将K2 V2转化为K3 V3
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //1：输出的K3为手机号，直接从K2转化即可
        //遍历集合，取出K3，并将K3和V3写入上下文中
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
