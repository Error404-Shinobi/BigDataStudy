





### MapReduce设计构思

一个完整的MapReduce程序在分布式运行时有三类实例进程：

1. MRAppMaster	负责整个程序的过程调度及状态协调
2. MapTask  负责map阶段的整个数据处理流程
3. ReduceTask  负责reduce阶段的整个数据处理流程

### MapReduce编程规范

> MapReduce的开发一共有八个步骤，其中Map阶段分为两个步骤，Shuffle阶段分为4个步骤，Reduce阶段分为两个步骤

**Map阶段2个步骤**

1. 设置InputFormat类，将数据切分为Key-Value(**K1和V1**)对，输入到第二步
2. 自定义Map逻辑，将第一步的结果转换成另外的Key-Value( **K2和V2** )对，输出结果

**Shuffle阶段4个步骤**

3. 对输出的Key-Value进行**分区**
4. 对不同分的数据按照相同的Key**排序**
5. (可选)对分组过的数据初步**规约**，降低数据的网络拷贝
6. 对数据进行**分组**，相同的Key的Value放入一个集合中

**Reduce阶段两个步骤**

7. 对多个Map任务的结果进行排序以及合并，编写Reduce函数实现自己的逻辑，对输入的Key-Value进行处理，转为新的Key-Value(**K3和V3**)输出

8. 设置OutputFormat处理并保存Reduce输出的Key-Value数据

### WordCount

> > 需求：在一堆给定的文本文件中统计输出每次一个单词出现的次数

**Step 1.数据格式准备**

1. 创建一个新的文件

   > cd /export/servers
   >
   > vi wordcount.txt

2. 向其中放入以下内容并保存

   >hello,world,hadoop
   >
   >hive,sqoop,flume,hello
   >
   >kitty,tom,jerry,world
   >
   >hadoop

3. 上传到HDFS

   > hdfs dfs -mkdir /wordcount/
   >
   > hdfs dfs -put wordcount.txt /wordcount/

**Step 2.Mapper **

> ```
> public class WordCountMapper extends Mapper <LongWritable, Text,Text,LongWritable> {
> 
>     //map方法就是将K1,V1转为K2,V2
>     /*
>     参数：
>         key     ：K1     行偏移量
>         value   ：V1     每一行的文本数据
>         context     ：   表示上下文对象
>      */
> 
>     /*
>         如何将K1,V1转化为K2,V2
>         K1      V1
>         0       hello
>         K2      V2
>         hello   1
>      */
>     @Override
>     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
>         Text text = new Text();
>         LongWritable longWritable = new LongWritable();
>         //这两步是为了提取值，省略context.write方法中定义
> 
> 
>         //1：将一行的文本数据进行拆分
>         String[] split = value.toString().split(",");
>         //2：遍历数组，组装K2和V2
>         for (String word : split) {
>             //3：将K2和V2写入上下文
>             text.set(word);
>             longWritable.set(1);
>             context.write(text,longWritable);
>         }
> 
>     }
> }
> ```

**Step 3.Reducer**

> ```
> public class WordCountReducer extends Reducer <Text, LongWritable,Text,LongWritable> {
>     //reduce方法作用：将新的K2和V2转为K3和V3，将K3和V3写入上下文中
>     /*
>     参数：
>         key     ：新K2     行偏移量
>         value   ：集合 新V2     每一行的文本数据
>         context     ：   表示上下文对象
>         -----------------------------
>         如何将K2,V2转化为K3,V3
>         新   K2      V2
>             hello   <1,1>
>         -----------------------------
>             K3      V3
>             hello   2
>      */
>     @Override
>     protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
>         long count = 0;
>         LongWritable longWritable = new LongWritable(count);
>         //1：遍历集合，将集合中的数字相加，得到V3
>         for (LongWritable value : values) {
>             count += value.get();
>         }
>         //2：将K3和V3写入上下文中
>         context.write(key,longWritable);
>     }
> }
> ```

**Step 4.定义主类，描述Jpb并提交Job**

> ```
> public class JobMain extends Configured implements Tool {
>     //该方法用于指定一个job任务
>     @Override
>     public int run(String[] strings) throws Exception {
>         //1：创建一个job任务对象
>         Job job = Job.getInstance(super.getConf(), "WordCount");
>         //2：配置job任务对象(八个步骤)
> 
>         //第一步：指定文件的读取方式和读取路径
>         job.setInputFormatClass(TextInputFormat.class);
>         TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount"));
> 
>         //第二步：指定Map阶段的处理方式和数据类型
>         job.setMapperClass(WordCountMapper.class);
>         //设置Map阶段K2的类型
>         job.setMapOutputKeyClass(Text.class);
>         //设置Map阶段V2的类型
>         job.setMapOutputValueClass(LongWritable.class);
> 
>         //第三，四，五，六采用默认方式处理
> 
>         //第七步：指定Reduce阶段的处理方式和数据类型
>         job.setReducerClass(WordCountReducer.class);
>         //设置K3的类型
>         job.setOutputKeyClass(Text.class);
>         //设置V3的类型
>         job.setOutputValueClass(LongWritable.class);
> 
>         //第八步：设置输出类型
>         job.setOutputFormatClass(TextOutputFormat.class);
>         //设置输出路径
>         TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/wordcount_out"));
> 
>         //等待任务结束
>         boolean bl = job.waitForCompletion(true);
> 
> 
>         return bl ? 0:1;
>     }
> 
>     public static void main(String[] args) throws Exception {
> 
>         Configuration configuration = new Configuration();
>         //启动job任务
>         int run = ToolRunner.run(configuration, new JobMain(), args);
>         System.exit(run);
>     }
> }
> ```

## MapReduce运行模式

##### 集群运行模式

1. 将MapReduce程序提交给Yarn集群，分发到很多节点上并发执行

2. 处理的数据和输出结果应该位于HDFS文件系统

3. 提交集群的实现步骤：将程序打成JAR包，并上传，然后在集群上用Hadoop命令启动

   打成jar包前需要先在maven中清除一下
   生成的jar包一般在target文件夹下

>hadoop jar MapReduce-1.0-SNAPSHOT.jar MapReduce_api.JobMain
>
>MapReduce-1.0-SNAPSHOT.jar	为生成的jar包名
>
>MapReduce_api.JobMain				为生成jar包的Reference路径
>

##### 本地运行模式

只需要将代码中的文件路径修改即可（前提是输入目录中需要存在需处理的数据，输出目录不能已存在），之后在主方法中run即可

## MapReduce分区

##### 分区步骤：

**Step 1.定义Mapper**

这个Mapper程序不做任何逻辑，也不对Key-Value做任何改变，只是接收数据，然后往下发送

>```
>public class PartitionMapper extends Mapper <LongWritable, Text,Text, NullWritable>{
>    //map方法就是将K1,V1转为K2,V2
>    @Override
>    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
>        context.write(value,NullWritable.get());
>    }
>}
>```

**Step 2.自定义Partitioner（分区的关键）**

主要逻辑就在这里，这也是这个案例的意义，通过Partitioner将数据分发给不同的Reducer

>```
>public class MyPartitioner extends Partitioner<Text, NullWritable> {
>    /*
>        1：定义分区规则
>        2：返回对应的分区编号（此案例判断依据为开奖数字是否大于15）
>     */
>    @Override
>    public int getPartition(Text text, NullWritable nullWritable, int i) {
>        //1：拆分行文本数据（K2），获取中奖字段值
>        String[] split = text.toString().split("\t");
>        String numStr =  split[5];//此处因为案例给出数据的索引按分隔符分割后是5
>        //2：判断中奖字段和15的关系，然后返回对应分区编号
>        if (Integer.parseInt(numStr) >15){
>            return 1;
>        }else {
>            return 0;
>        }
>    }
>}
>```

**Step 3.定义Reducer逻辑**

这个Reducer也不做任何处理，将数据原封不动的输出即可

> ```
> public class PartitionerReducer extends Reducer <Text, NullWritable,Text,NullWritable> {
>     @Override
>     protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
>         context.write(key,NullWritable.get());
>     }
> }
> ```

**Step 4.主类中设置分区和ReduceTask个数**

> ```
> public class JobMain extends Configured implements Tool {
>     @Override
>     public int run(String[] strings) throws Exception {
>         //1：创建Job任务对象
>         Job job = Job.getInstance(super.getConf(), "Partition_MapReduce");
>         //2：对job任务进行配置（八个步骤）
>             //第一步：设置输入类和输入路径
>             job.setInputFormatClass(TextInputFormat.class);
>             TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input"));
>             //第二步：设置Mapper类和数据类型(K2和V2)
>             job.setMapperClass(PartitionMapper.class);
>             job.setOutputKeyClass(Text.class);
>             job.setOutputValueClass(NullWritable.class);
>             //第三步：指定分区类
>             job.setPartitionerClass(MyPartitioner.class);
>             //第四，五，六步采用默认方式
>             //第七步：指定Reducer类和数据类型(K3和V3)
>             job.setReducerClass(PartitionerReducer.class);
>             job.setOutputKeyClass(Text.class);
>             job.setOutputValueClass(NullWritable.class);
>             //设置ReduceTask个数
>             job.setNumReduceTasks(2);
>             //第八步：指定输出类和输出路径
>             job.setOutputFormatClass(TextOutputFormat.class);
>             TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/partition_out"));
>         //3：等待任务结束
>         boolean bl = job.waitForCompletion(true);
>         return bl?0:1;
>     }
> 
>     public static void main(String[] args) throws Exception {
>         Configuration configuration = new Configuration();
>         //启动job任务
>         int run = ToolRunner.run(configuration, new JobMain(), args);
>         System.exit(run);
>     }
> }
> ```

## MapReduce中的计数器

Hadoop内置计数器列表

| MapReduce任务计数器    | org.apache.hadoop.mapreduce.TaskCounter                      |
| ---------------------- | ------------------------------------------------------------ |
| 文件系统计数器         | org.apache.hadoop.mapreduce.FileSystemCounter                |
| FileInputFormat计数器  | org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter |
| FileOutputFormat计数器 | org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter |
| 作业计数器             | org.apache.hadoop.mapreduce.JobCounter                       |

##### 自定义计数器

###### 第一种方式

**第一种方式定义计数器，通过context上下文对象可以获取计数器，进行记录，通过context上下文对象，在map端使用计数器进行统计**

> ```
> public class Mapper_Counter1 extends Mapper <LongWritable,Text,Text,NullWritable> {
>     @Override
>     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
>         //MR_COUNTER为自定义计数器类型的名称，partition_counter为自定义计数器输出变量的名称
>         Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
>         //每次执行该方法则计数器变量的值加一（变量为Long类型）
>         counter.increment(1L);
>         context.write(value,NullWritable.get());
>     }
> }
> ```

###### 第二种方式

**通过enum枚举类型来定义计数器**  统计reduce端数据的输入的key有多少个

> ```
> public class Reducer_Counter2 extends Reducer<Text, NullWritable,Text,NullWritable> {
>     public static enum Counter{
>         MY_INPUT_RECOREDS,MY_INPUT_BYTES
>     }
>     @Override
>     protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
>         //枚举定义计数器
>         context.getCounter(Counter.MY_INPUT_RECOREDS).increment(1L);
>         context.write(key,NullWritable.get());
>     }
> }
> ```

## MapReduce排序和序列化

- 序列化（Serialization）是指把结构化对象转化为字节流
- 反序列化（Deserialization）是序列化的逆过程，把字节流转为结构化对象。当要在进程间传递处对象或持久化对象的时候，就需要序列化对象成字节流，反之当要将接收到或从磁盘读取的字节转换为对象，就要进行反序列化
- Java的序列化（Serializable）是一个重量级序列化框架，一个对象被序列化成后，会附带很多额外的信息（各种校验信息，header，继承体系等），不便于在网络中高效传输。所以，Hadoop自己开发了一套序列化机制（Writable），精简高效，不用像Java对象类一样传输多层的父子关系，需要哪个属性就传输哪个属性值，大大减少网络传输的开销
- Writable是Hadoop的序列化格式，Hadoop定义了这样一个Writable接口。一个类要支持可序列化只需要实现这个接口即可
- 另外Writable有一个子接口是WritableComparable，WritableComparable是既可实现序列化，也可以对key进行比较，我们这里可以通过自定义Key实现WritableComparable来实现我们的排序功能

数据格式如下

> a		1
>
> a		9
>
> b		3
>
> a		7
>
> b		8
>
> b		10
>
> a		5

要求：

- 第一列按照字典顺序进行排列
- 第一列相同的时候，第二列按照升序进行排列

##### 解决思路

**Step 1.自定义类型和比较器**

> ```
> public class SortBean implements WritableComparable <SortBean> {
>     private String word;
>     private int num;
> 
>     public String getWord() {
>         return word;
>     }
> 
>     public void setWord(String word) {
>         this.word = word;
>     }
> 
>     public int getNum() {
>         return num;
>     }
> 
>     public void setNum(int num) {
>         this.num = num;
>     }
> 
>     @Override
>     public String   toString() {
>         return  word + "\t" + num;
>     }
> 
>     //实现比较器，指定排序的规则
>     /*
>         第一列（word）按照字典顺序进行排列
>         第一列相同的时候，第二列（num）按照升序进行排列
>      */
>     @Override
>     public int compareTo(@NotNull SortBean sortBean) {
>         //先对word排列
>         int result = this.word.compareTo(sortBean.word);
>         //如果word相同，则按照num进行排序
>         if (result == 0) {
>             return this.num - sortBean.num;
>         }
>         return result;
>     }
> 
>     //实现序列化
>     @Override
>     public void write(DataOutput out) throws IOException {
>         out.writeUTF(word);
>         out.writeInt(num);
>     }
> 
>     //实现反序列化
>     @Override
>     public void readFields(DataInput in) throws IOException {
>         this.word=in.readUTF();
>         this.num=in.readInt();
>     }
> }
> ```

**Step 2.Mapper**

> ```
> public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
>     /*
>         K1          V1
>         0          a  3
>         5          b  7
>         ----------------
>         K2                          V2
>        SortBean(a  3)         NullWritable
>        SortBean(b  7)         NullWritable
>      */
>     @Override
>     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
>         //1：将行文本数据V1拆分，并将数据封装到SortBean对象，得到K2
>         String[] split = value.toString().split("\t");
>         SortBean sortBean = new SortBean();
>         sortBean.setWord(split[0]);
>         sortBean.setNum(Integer.parseInt(split[1]));
>         //2：将K2和V2写入上下文中
>         context.write(sortBean,NullWritable.get());
>     }
> }
> ```

**Step 3.Reducer**

> ```
> public class SortReducer extends Reducer<SortBean, NullWritable,SortBean,NullWritable> {
>     @Override
>     protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
>         context.write(key,NullWritable.get());
>     }
> }
> ```

**Step 4.Main入口**

> ```
> public class JobMain extends Configured implements Tool {
>     @Override
>     public int run(String[] strings) throws Exception {
>         //1：创建Job对象
>         Job job = Job.getInstance(super.getConf(), "mapreduce_sort");
>         //2：配置Job任务(八个步骤)
>             //第一步：设置输入类和输入路径
>             job.setInputFormatClass(TextInputFormat.class);
>             //TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input/sort_input"));
>             TextInputFormat.addInputPath(job,new Path("file:////Users/xuansama/Desktop/input"));
>             //第二步：设置Mapper类和数据类型
>             job.setMapperClass(SortMapper.class);
>             job.setMapOutputKeyClass(SortBean.class);
>             job.setMapOutputValueClass(NullWritable.class);
>             //第三、四、五、六
> 
>             //第七步：设置Reducer类和数据类型
>             job.setReducerClass(SortReducer.class);
>             job.setOutputKeyClass(SortBean.class);
>             job.setOutputValueClass(NullWritable.class);
> 
>             //第八步：设置输出类和输出路径
>             job.setOutputFormatClass(TextOutputFormat.class);
>             //TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/sort_out"));
>             TextOutputFormat.setOutputPath(job,new Path("file:////Users/xuansama/Desktop/output/sort_output2"));
>         //3：等待任务结束
>         boolean bl = job.waitForCompletion(true);
>         return bl?0:1;
>     }
> 
>     public static void main(String[] args) throws Exception {
>         Configuration configuration = new Configuration();
>         ToolRunner.run(configuration,new JobMain(),args);
>     }
> }
> ```

## 规约Combiner

**概念**

每个map都可能会产生大量的本地输出，Combiner的作用就是对map端的输出先做一次合并，以减少在map和reduce节点之间的数据传输量，以提高网络IO性能，是MapReduce的一中优化手段之一

- combiner是MR程序中Mapper和Reducer之外的一中组件
- combiner组件的父类就是Reducer
- combiner和reducer的区别在于运行的位置
- - Combiner是在没一个maptask所在的节点运行
  - Reducer是接受全局所有Mapper的输出结果
- combiner的意义就是对没一个maptask的输出进行局部汇总，以减小网络传输量

**实现步骤**

1. 自定义一个combiner继承Reducer，重写reduce方法
2. 在job中设置  job.setCombinerClass(CustomCombiner.class)



combiner 能够应用的前提是不能影响最终的业务逻辑，而且，combiner的输出KV应该和reduce的输入KV类型对应











