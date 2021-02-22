### 启动zookeeper集群

三台机器都要启动

- /export/servers/zookeeper-3.5.9/bin/zkServer.sh start
- 查看启动状态：/export/servers/zookeeper-3.5.9/bin/zkServer.sh status

启动客户端

- bin/zkCli.sh -server node01:2181

### 启动hadoop集群

- 首先要确保zookeeper集群已启动
- cd /export/servers/hadoop-2.10.1/
- bin/hdfs namenode -format         (此条命令为第一次启动集群时使用，后续不需要)
- /export/servers/hadoop-2.10.1/sbin/start-dfs.sh
- /export/servers/hadoop-2.10.1/sbin/start-yarn.sh
- /export/servers/hadoop-2.10.1/sbin/mr-jobhistory-daemon.sh start historyserver
- 启动Hbase
  - /export/servers/hbase-2.3.4/bin/start-hbase.sh
- 关闭HRegion
  - /export/servers/hbase-2.3.4/bin/hbase-daemon.sh stop regionserver RegionServer

### 三个端口查看界面

- http://172.16.74.100:50070/explorer.html#/查看hdfs
- http://172.16.74.100:8088/cluster查看yarn集群
- http://172.16.74.100:19888/jobhistory查看历史完成的任务
- http://172.16.74.100:16010/master-status查看hbase界面
- 注：如果在主机端口（即实体机非虚拟机）host中映射了虚拟机IP可直接用node01代替IP



### hdfs命令行使用

注：有三种命令使用方式：

hadoop fs：适用于不同的本地文件系统，也适用于hdfs

hadoop dfs：只适用于hdfs

hdfs dfs：只适用于hdfs

- ls

  - 格式：hdfs dfs -ls URI		URI指路径
  - 作用：类似于Linux的ls命令，显示文件列表

- lsr

  - 格式：hdfs dfs -lsr URI
  - 作用：在整个目录下递归执行ls，与UNIX中的ls-R类似

- mkdir

  - 格式：hdfs dfs [-p] -mkdir <paths>
  - 作用：以<path>中的URI作为参数，创建目录。使用-p参数可以递归创建目录

- put

  - hdfs dfs -put <localsrc>   <dst>
  - 作用：将单个的源文件src或者多个源文件srcs从本地文件系统拷贝到目标文件系统中(<dst>对应的路径)。也可以从标准输入中读取输入，写入目标文件系统中
  - 例：hdfs dfs -put /root/a.txt /dir1

- moveFromLocal

  - 格式：hdfs dfs -moveFromlocal <localsrc>   <dst>

  - 作用：和put命令类似，但是源文件localsrc拷贝之后自身被删除

    注：move和put不同之处在于put类似于拷贝，move类似于剪切

- moveToLocal

  - 暂时未实现

- get

  - 格式：hdfs dfs -get [-ignorecrc] [-crc] <src> <localdst>
  - 作用：将文件拷贝到本地文件系统。CRC 校验失败的文件通过-ignorecrc选项拷贝。文件和CRC校验和 可以通过-CRC选项拷贝
  - 例：hdfs dfs -get /dir2/a.txt ./

- getmerge

  - 格式：hdfs dfs -getmerge /config/*.xml*  ./hello.xml
  - 作用：将hdfs小文件合并到hello.xml并且下载到本地(指的是linux系统中执行命令的目录下)

- mv

  - 格式：hdfs dfs -mv URI <dest>
  - 作用：将hdfs上的文件从原路径移动到目标路径（移动之后文件删除），该命令不能跨文件系统

- rm

  - 格式：hdfs dfs -rm [-r] [-skipTrash] URI [URI]
  - 作用：删除参数指定的文件，参数可以有多个
    - 如果有指定-skipTrash选项，那么在回收站可用的情况下，该选项将跳过回收站而直接删除文件；
    - 否则，在回收站可用时，在HDFS Shell中执行此命令，会将文件暂时放到回收站中
  - 例：hdfs dfs -rm -r /dir1

- cp

  - 格式：hdfs dfs -cp URI [URI...] <dest>
  - 作用：将文件拷贝到目标路径中。如果<dest>为目录的话，可以讲多个文件拷贝到该目录下
  - -f
    - 选项将覆盖目标，如果它已经存在
  - -p
    - 选项将保留文件属性（时间戳、所有权、许可、ACL、XAttr）

- cat

  - 格式：hdfs dfs -cat URI [uri ...]
  - 作用：将参数所指示的文件内容输出到stdout

- chmod

  - 格式：hdfs dfs -chmod [-R] URI[URI...]
  - 作用：改变文件权限，如果使用 -R 选项，则对整个目录有效递归执行。使用这一命令的用户必须是文件的所属用户，或者超级用户。
  - 例：hdfs dfs -chmod -R 777 /install.log

- chown

  - 格式：hdfs dfs -chown [-R] URI[URI ...]
  - 作用：改变文件所属的用户和用户组。如果使用 -R 选项，则对整个目录有效递归执行。使用这一命令的用户必须是文件的所属用户，或者超级用户。
  - 例：hdfs dfs -chown -R hadoop:hadoop /install.rog
    - 第一个hadoop为用户，第二个为用户组

- appendToFile

  - 格式：hdfs dfs -appendToFile <localsrc> ... <dst>
  - 作用：追加一个或多个本地文件到hdfs指定文件中，也可以命令行读取输入
  - 例：hdfs dfs -appendToFile a.xml b.xml /big.xml

## hdfs的高级使用命令

1. HDFS文件限额配置

   hdfs dfs -count -q -h /user/root/dir1 #查看配额信息

   - 数量限额
     - hdfs dfs -mkdir -p /user/root/dir	#创建hdfs文件夹
     - hdfs dfsadmin -setQuota 2 dir      #给该文件夹设置最多上传2个文件
     - hdfs dfsadmin -clrQuota /user/root/dir     #清除数量限制
     - 注：无论设置多少个文件，可用的永远是n-1个，因为目录本身被视为一个文件
       - 在hdfs文件系统中，默认的路径是/user/root/，相对路径以它为初始位置
   - 空间大小限额
   - 注：在设置空间配额时，设置的空间至少是block_size*3大小
     - hdfs dfsadmin -setSpaceQuota 4k /user/root/dir	#限制空间大小为4KB
     - hdfs dfsadmin -clrSpaceQuota /user/root/dir     #清除空间大小限制
   - 生成大小文件的命令：
     - dd if=/dev/zero of=1.txt bs=1M count=2	#生成2M的文件

2. HDFS的安全模式
   安全模式是Hadoop的一种保护机制，用于保证集群中的数据块的安全性。当集群启动时，会首先进入安全模式。当系统处于安全模式时会检查数据块的完整性。
   在安全模式状态下，文件系统只接受读数据请求，而不接受删除、修改等变更请求。在整个系统到达安全标准时，HDFS自动离开安全模式（默认30秒）
   - 安全模式操作命令
     - hdfs dfsadmin -safemode get	#查看安全模式状态
     - hdfs dfsadmin -safemode enter	#进入安全模式
     - hdfs dfsadmin -safemode leave	#离开安全模式

3. HDFS基准测试
   - 测试写入速度
     - 向HDFS文件系统中写入数据，10个文件，每个文件10MB，文件存放到/benchmarks/TestDFSIO中
     - hadoop jar /export/servers/hadoop-xxx/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclicent-xxx.jar TestDFSIO -write -nrFiles 10 -fileSize 10MB
     - 注：-write 表示写入，-nrFiles 表示xx个文件，-fileSize表示每个文件大小
     - 完成之后查看写入速度结果
     - hdfs dfs -text /benchmarks/TestDFSIO/io_write/part-00000
   - 测试读取速度
     - hadoop jar /export/servers/hadoop-xxx/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclicent-xxx.jar TestDFSIO -read -nrFiles 10 -fileSize 10MB
     - 完成之后查看读取速度结果
     - hdfs dfs -text /benchmarks/TestDFSIO/io_read/part-00000
   - 清除测试数据
     - hadoop jar /export/servers/hadoop-xxx/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclicent-xxx.jar TestDFSIO -clean

## java api 操作

获取FileSystem的方式

- ```UTF-8
  /*
  获取FileSystem；方式1
  * */
  @Test
  public void getFileSystem1() throws IOException {
      //1：创建Configuration对象
      Configuration configuration = new Configuration();
      //2：设置文件系统类型
      configuration.set("fs.defaultFS","hdfs://172.16.74.100:8020/");
      //3：获取指定文件系统
      FileSystem fileSystem = FileSystem.get(configuration);
  
      //4：输出
      System.out.println(fileSystem);
  }
  ```

- ```UTF-8
  /*
  获取FileSystem；方式2
  * */
  @Test
  public void getFileSystem2() throws URISyntaxException, IOException {
      FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      System.out.println(fileSystem);
  }
  ```

- ```UTF-8
  /*
  获取FileSystem；方式3
  * */
  @Test
  public void getFileSystem3() throws IOException {
      Configuration configuration = new Configuration();
      configuration.set("fs.defaultFS","hdfs://172.16.74.100:8020/");
      FileSystem fileSystem = FileSystem.newInstance(configuration);
      System.out.println(fileSystem.toString());
  }
  ```

- ```UTF-8
  /*
     获取FileSystem；方式4
     * */
  @Test
  public void getFileSystem4() throws URISyntaxException, IOException {
      FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      System.out.println(fileSystem.toString());
  }
  ```

遍历HFDFS中所有文件

- ```UTF-8
  /*
  hdfs文件遍历
  * */
  @Test
  public void listFiles() throws URISyntaxException, IOException {
      //1：获取fileSystem实例
      FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      //2：调用listFiles方法获取根目录下所有信息
      RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
      //3：遍历迭代器
      while (iterator.hasNext()){
          LocatedFileStatus fileStatus = iterator.next();
          //获取文件的绝对路径：hdfs：//172.16.74.100：8020/xxx
          System.out.println(fileStatus.getPath()+"-----"+fileStatus.getPath().getName());
          //获取文件被切分成多少个block,文件的block信息
          BlockLocation[] blockLocations = fileStatus.getBlockLocations();
          System.out.println("block数："+blockLocations.length);
      }
  }
  ```

  HDFS创建文件夹/文件

  ```UTF-8
  /*
      hdfs上创建文件夹
       */
      @Test
      public void mkdirsTest() throws URISyntaxException, IOException {
          //1：获取FileSystem实例
          FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
          //2：创建文件夹
  //        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mydir/test"));
  //        System.out.println(mkdirs);
          //3：创建文件
          fileSystem.create(new Path("/hello/mydir/test/aaa.txt"));
          fileSystem.close();
      }
  ```

HDFS文件下载与上传

- ```UTF-8
  /*
  实现文件下载
   */
  @Test
  public void downloadFile() throws URISyntaxException, IOException {
      //1：获取FileSystem
      FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      //2：获取hdfs的输入流
      FSDataInputStream inputStream = fileSystem.open(new Path("/dir1/a.txt"));
      //3：获取本地路径的输出流
      FileOutputStream outputStream = new FileOutputStream("/Users/xuansama/Desktop/a.txt");
      //4：文件拷贝
      IOUtils.copy(inputStream,outputStream);
      //5：关闭流
      IOUtils.closeQuietly(inputStream);
      IOUtils.closeQuietly(outputStream);
      fileSystem.close();
  }
  
  /*
  文件下载：方式2
   */
  @Test
  public void downloadFile2() throws URISyntaxException, IOException {
      //1：获取FileSystem
      FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      //2：调用方法，实现文件下载
      fileSystem.copyToLocalFile(new Path("/dir1/a.txt"),new Path("/Users/xuansama/Desktop/a.txt"));
      //3：关闭fileSystem
      fileSystem.close();
  }
  ```

  ```UTF-8
  /*
  文件上传
   */
  @Test
  public void uploadFile() throws URISyntaxException, IOException {
      FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
      fileSystem.copyFromLocalFile(new Path("/Users/xuansama/Desktop/a.txt"),new Path("/dir2/a.txt"));
      fileSystem.close();
  }
  ```

HDFS访问权限控制

- 在远程用户权限不够的情况下可使用FileSystem.get(URI,Configuration,user)的方式伪造用户，从而实现强行读写

HDFS合并小文件实现上传

```UTF-8
/*
小文件的合并
 */
@Test
public void mergeFile() throws URISyntaxException, IOException {
    //1：获取FileSystem（分布式文件系统）
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
    //2：获取hdfs大文件的输出流
    FSDataOutputStream outputStream = fileSystem.create(new Path("/big_txt.txt"));
    //3：获取本地文件系统
    LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    //4：获取本地文件夹下所有文件详情
    FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/Users/xuansama/Desktop/input"));
    //5：遍历每个文件，获取每个文件的输入流
    for (FileStatus fileStatus : fileStatuses) {
        FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
        //6：将小文件的数据复制到大文件
        IOUtils.copy(inputStream,outputStream);
        IOUtils.closeQuietly(inputStream);
    }
    //7：关闭流
    IOUtils.closeQuietly(outputStream);
    localFileSystem.close();
    fileSystem.close();
}
```

HDFS查看是否存在一个文件

```UTF-8
/*
测试文件是否存在
 */
@Test
public void ifexist() throws URISyntaxException, IOException {
    String filename="hdfs://172.16.74.100:8020/big_txt.txt";
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
    if(fileSystem.exists(new Path(filename))){
        System.out.println("文件存在");
    }
    else {
        System.out.println("文件不存在");
    }
}
```



