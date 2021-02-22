package MapReduce_api;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/15 - 11:12 上午
 */

/*
    Mapper的四个泛型：
    KEYIN：K2的类型,MapReduce自定义字符类型  Text
    VALUEIN：V2的类型，MapReduce有自己的定义 LongWritable
    KEYOUT：K3的类型
    VALUEOUT：V3的类型
*/
public class WordCountReducer extends Reducer <Text, LongWritable,Text,LongWritable> {
    //reduce方法作用：将新的K2和V2转为K3和V3，将K3和V3写入上下文中
    /*
    参数：
        key     ：新K2     行偏移量
        value   ：集合 新V2     每一行的文本数据
        context     ：   表示上下文对象
        -----------------------------
        如何将K2,V2转化为K3,V3
        新   K2      V2
            hello   <1,1>
        -----------------------------
            K3      V3
            hello   2
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        //LongWritable longWritable = new LongWritable(count);
        //1：遍历集合，将集合中的数字相加，得到V3
        for (LongWritable value : values) {
            count += value.get();
        }
        //2：将K3和V3写入上下文中
        context.write(key,new LongWritable(count));
    }
}
