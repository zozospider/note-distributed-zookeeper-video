package com.zozospider.zookeeperjavaclient.zk.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ZKNodeOperator2 {

    final static Logger log = LoggerFactory.getLogger(ZKNodeOperator2.class);

    /**
     * server列表, 以逗号分割
     */
    protected String connectString = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    protected ZooKeeper zookeeper;

    /**
     * 连接zookeeper server
     */
    public void connect() throws Exception {
        // 创建连接
        log.info("create ZooKeeper begin");
        zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZKNodeOperator2.ConnWatcher());
        log.info("create ZooKeeper end");

        // 等待连接完成
        log.info("create ZooKeeper countDownLatch begin");
        countDownLatch.await();
        log.info("create ZooKeeper countDownLatch end");
    }

    public class ConnWatcher implements Watcher {
        public void process(WatchedEvent event) {
            log.info("ConnWatcher 接受到watch通知：{}", event);
            // 连接建立, 回调process接口时, 其event.getState()为KeeperState.SyncConnected
            if (event.getState() == Event.KeeperState.SyncConnected) {
                log.info("ConnWatcher 接受到watch通知：SyncConnected");
                // 放开闸门, wait在connect方法上的线程将被唤醒
                countDownLatch.countDown();
            }
        }
    }

    /**
     * 创建节点（同步）
     * @param path 节点路径
     * @param data 节点数据
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String path, String data) throws KeeperException, InterruptedException {

        log.info("create begin");
        // Ids.OPEN_ACL_UNSAFE  ->  world:anyone:cdrwa
        // CreateMode.PERSISTENT  ->  创建持久节点
        zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("create end");
    }

    /**
     * 创建节点（异步）
     * @param path 节点路径
     * @param data 节点数据
     * @throws InterruptedException
     */
    public void createAsync(String path, String data) throws InterruptedException {

        log.info("createAsync begin");
        // Ids.OPEN_ACL_UNSAFE  ->  world:anyone:cdrwa
        // 创建持久节点
        zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                log.info("createAsync rc: {}, path: {}, ctx: {}, name: {}", rc, path, ctx, name);
            }
        }, "createAsync Object ctx");
        log.info("createAsync end");
        Thread.sleep(5000);
    }

    /**
     * 修改节点数据（同步）
     * @param path 节点路径
     * @param newData 修改后节点数据
     * @param version 当前版本
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void setData(String path, String newData, int version) throws KeeperException, InterruptedException {
        log.info("setData begin");
        Stat stat = zookeeper.setData(path, newData.getBytes(), version);
        log.info("setData stat: " + stat);
        log.info("setData end");
    }

    /**
     * 修改节点数据（异步）
     * @param path 节点路径
     * @param newData 修改后的节点数据
     * @param version 当前版本
     * @throws InterruptedException
     */
    public void setDataAsync(String path, String newData, int version) throws InterruptedException {
        log.info("setDataAsync begin");
        zookeeper.setData(path, newData.getBytes(), version, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                log.info("setDataAsync rc: {}, path: {}, ctx: {}, stat: {}", rc, path, ctx, stat);
            }
        }, "setDataAsync Object ctx");
        log.info("setDataAsync end");
        Thread.sleep(5000);
    }

    public void delete() {

    }

    public static void main(String[] args) throws Exception {
        ZKNodeOperator2 zKNodeOperator2 = new ZKNodeOperator2();
        zKNodeOperator2.connect();
//        zKNodeOperator2.create("/zKNodeOperator2", "zKNodeOperator2 data");
//        zKNodeOperator2.createAsync("/zKNodeOperator2", "zKNodeOperator2 data");
//        zKNodeOperator2.setData("/zKNodeOperator2", "zKNodeOperator2 newData", 0);
        zKNodeOperator2.setDataAsync("/zKNodeOperator2", "zKNodeOperator2 newData1", 1);
    }

}
