package hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author xuansama
 * @date 2021/1/24 - 10:37 下午
 */
public class hdfsApiDemo {
    @Test
    public void urlHdfs() throws IOException {
        //1:注册URL
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //2:获取hdfs文件的输入流
        InputStream inputStream =new URL("hdfs://172.16.74.100:8020/dir1/a.txt").openStream();
        //3:获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream(new File("/Users/xuansama/Desktop/hello.txt"));
        //4:实现文件拷贝
        IOUtils.copy(inputStream,outputStream);
        //5:关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

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

     /*
    获取FileSystem；方式2
    * */
    @Test
    public void getFileSystem2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
        System.out.println(fileSystem);
    }

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


    /*
       获取FileSystem；方式4
       * */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://172.16.74.100:8020/"), new Configuration());
        System.out.println(fileSystem.toString());
    }
}
