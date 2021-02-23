package Flow_Partition_demo3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/22 - 7:26 下午
 */
public class FlowCountReducer extends Reducer <Text, FlowBean, Text, FlowBean> {
    /*

     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        //1：遍历集合，并将集合中的对应的四个字段累计
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        //2：创建FlowBean对象，并给对象赋值
        FlowBean bean = new FlowBean();
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setUpCountFlow(upCountFlow);
        bean.setDownCountFlow(downCountFlow);

        //3：将K3和V3写入上下文中
        context.write(key, bean);
    }
}
