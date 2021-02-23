package Flow_Sort_demo2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author xuansama
 * @Date 2021/2/22 - 8:24 下午
 */
public class JobMain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //1：创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "Flow_Sort");
        //2：配置job任务对象(八个步骤)

        //第一步：指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:////Users/xuansama/Desktop/output/flowcount_out"));

        //第二步：指定Map阶段的处理方式和数据类型
        job.setMapperClass(FlowSortMapper.class);
        //设置Map阶段K2的类型
        job.setMapOutputKeyClass(FlowBean.class);
        //设置Map阶段V2的类型
        job.setMapOutputValueClass(Text.class);


        //第七步：指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(FlowSortReducer.class);
        //设置K3的类型
        job.setOutputKeyClass(Text.class);
        //设置V3的类型
        job.setOutputValueClass(FlowBean.class);

        //第八步：设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出路径
        Path path = new Path("file:////Users/xuansama/Desktop/output/flowsort_out");
        TextOutputFormat.setOutputPath(job, path);

        //等待任务结束
        boolean bl = job.waitForCompletion(true);


        return bl ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
