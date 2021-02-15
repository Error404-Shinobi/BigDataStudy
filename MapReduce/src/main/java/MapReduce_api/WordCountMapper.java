package MapReduce_api;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/15 - 10:53 上午
 */

/*
    Mapper的四个泛型：
    KEYIN：K1的类型,MapReduce有自己的定义 LongWritable
    VALUEIN：V1的类型，MapReduce自定义字符类型  Text
    KEYOUT：K2的类型
    VALUEOUT：V2的类型
*/
public class WordCountMapper extends Mapper <LongWritable, Text,Text,LongWritable> {

    //map方法就是将K1,V1转为K2,V2
    /*
    参数：
        key     ：K1     行偏移量
        value   ：V1     每一行的文本数据
        context     ：   表示上下文对象
     */

    /*
        如何将K1,V1转化为K2,V2
        K1      V1
        0       hello
        K2      V2
        hello   1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        //这两步是为了提取值，省略context.write方法中定义


        //1：将一行的文本数据进行拆分
        String[] split = value.toString().split(",");
        //2：遍历数组，组装K2和V2
        for (String word : split) {
            //3：将K2和V2写入上下文
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }

    }
}
