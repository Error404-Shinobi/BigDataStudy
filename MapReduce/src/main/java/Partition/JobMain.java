package Partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 11:20 上午
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //1：创建Job任务对象
        Job job = Job.getInstance(super.getConf(), "Partition_MapReduce");
        //2：对job任务进行配置（八个步骤）
            //第一步：设置输入类和输入路径
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input"));
            //第二步：设置Mapper类和数据类型(K2和V2)
            job.setMapperClass(PartitionMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //第三步：指定分区类
            job.setPartitionerClass(MyPartitioner.class);
            //第四，五，六步采用默认方式
            //第七步：指定Reducer类和数据类型(K3和V3)
            job.setReducerClass(PartitionerReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //设置ReduceTask个数
            job.setNumReduceTasks(2);
            //第八步：指定输出类和输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/partition_out"));
        //3：等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
