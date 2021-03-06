package com.zozospider.zookeeperjavaclient.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper 连接 demo2（更合理）
 */
public class ZKConnect2 {

    final static Logger log = LoggerFactory.getLogger(ZKConnect2.class);

    /**
     * server列表, 以逗号分割
     */
    protected String connectString = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";
    /**
     * 连接的超时时间, 毫秒
     */
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    protected ZooKeeper zookeeper;

    /**
     * 连接zookeeper server
     */
    public void connect() throws Exception {
        // 创建连接
        log.info("create ZooKeeper begin");
        zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZKConnect2.ConnWatcher());
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

    public static void main(String[] args) throws Exception {
        new ZKConnect2().connect();
    }

}
