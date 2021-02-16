package Sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 7:59 下午
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //1：创建Job对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_sort");
        //2：配置Job任务(八个步骤)
            //第一步：设置输入类和输入路径
            job.setInputFormatClass(TextInputFormat.class);
            //TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input/sort_input"));
            TextInputFormat.addInputPath(job,new Path("file:////Users/xuansama/Desktop/input"));
            //第二步：设置Mapper类和数据类型
            job.setMapperClass(SortMapper.class);
            job.setMapOutputKeyClass(SortBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            //第三、四、五、六

            //第七步：设置Reducer类和数据类型
            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(SortBean.class);
            job.setOutputValueClass(NullWritable.class);

            //第八步：设置输出类和输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            //TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/sort_out"));
            TextOutputFormat.setOutputPath(job,new Path("file:////Users/xuansama/Desktop/output/sort_output2"));
        //3：等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(configuration,new JobMain(),args);
    }
}
