package zookeeper_api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author xuansama
 * @Date 2021/1/28 - 11:45 上午
 */
public class zookeeperApiDemo {
    private static final String connectString="node01:2181,node02:2181,node03:2181";
    private static final int sessionTimeout=30000;
    private ZooKeeper zkClient;

    //连接服务器
    @Test
    public void init() throws IOException {
        zkClient= new ZooKeeper(connectString, sessionTimeout, new Watcher(){
            @Override
            public void process(WatchedEvent event){

            }
        });
    }
    //创建子节点
    @Test
    public void createNode() throws KeeperException, InterruptedException, IOException {
        zkClient = new ZooKeeper("node01:2181",15000,null);
        String createNode= zkClient.create("/xuansama","whisper".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(createNode);
    }

    //获取子节点并监听子节点变化
    @Test
    public void getChildren() throws KeeperException, InterruptedException, IOException {
        Watcher event;
        event = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("------start-----");
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/",true);
                    for (String child:children){
                        System.out.println(child);
                    }
                    System.out.println("------end-------");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        zkClient = new ZooKeeper("node01:2181",15000,event);
//        List<String> children = zkClient.getChildren("/",false);
//        for (String child:children){
//            System.out.println(child);
//        }

        //延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    // 判断 znode 是否存在
    @Test
    public void exist() throws Exception {
        zkClient = new ZooKeeper("node01:2181",15000,null);
        Stat stat = zkClient.exists("/eclipse", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
