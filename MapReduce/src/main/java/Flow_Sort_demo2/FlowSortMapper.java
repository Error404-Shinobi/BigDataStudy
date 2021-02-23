package Flow_Sort_demo2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/23 - 2:29 下午
 */
public class FlowSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    //map方法:将K1和V1转为K2和V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        //1：创建FlowBean对象，并给每个字段赋值，得到K2
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        //2：定义一个变量，得到手机号V2
        String phoneNum = split[0];
        //3：将K2和V2写入上下文中
        context.write(flowBean,new Text(phoneNum));
    }
}
