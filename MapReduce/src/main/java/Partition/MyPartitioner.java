package Partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 11:08 上午
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /*
        1：定义分区规则
        2：返回对应的分区编号（此案例判断依据为开奖数字是否大于15）
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //1：拆分行文本数据（K2），获取中奖字段值
        String[] split = text.toString().split("\t");
        String numStr =  split[5];//此处因为案例给出数据的索引按分隔符分割后是5
        //2：判断中奖字段和15的关系，然后返回对应分区编号
        if (Integer.parseInt(numStr) >15){
            return 1;
        }else {
            return 0;
        }
    }
}
