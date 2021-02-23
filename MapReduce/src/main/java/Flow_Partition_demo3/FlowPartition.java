package Flow_Partition_demo3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author xuansama
 * @Date 2021/2/23 - 3:21 下午
 */
public class FlowPartition extends Partitioner<Text,FlowBean> {
    /*
      该方法用来指定分区的规则:
        135 开头数据到一个分区文件
        136 开头数据到一个分区文件
        137 开头数据到一个分区文件
        其他分区

       参数:
         text : K2   手机号
         flowBean: V2
         i   : ReduceTask的个数
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //1：获取手机号
        String phoneNum = text.toString();
        //2：判断分区
        if (phoneNum.startsWith("135")){
            return 0;
        }else if (phoneNum.startsWith("136")){
            return 1;
        }else if (phoneNum.startsWith("137")){
            return 2;
        }else {
            return 3;
        }
    }
}
