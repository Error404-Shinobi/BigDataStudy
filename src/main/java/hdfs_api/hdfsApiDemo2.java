package hdfs_api;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author xuansama
 * @date 2021/1/25 - 3:20 下午
 */
public class hdfsApiDemo2 {
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

    /*
    文件上传
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("/Users/xuansama/Desktop/a.txt"),new Path("/dir2/a.txt"));
        fileSystem.close();
    }

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
}
