package com.zozospider.zookeeperjavaclient.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper 会话重连 Demo2（更合理）
 */
public class ZKConnectSessionWatcher2 {

    final static Logger log = LoggerFactory.getLogger(ZKConnectSessionWatcher.class);

    public static final String connectString = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";
    /**
     * 连接的超时时间, 毫秒
     */
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private CountDownLatch countDownLatch2 = new CountDownLatch(1);
    protected ZooKeeper zookeeper;
    protected ZooKeeper zookeeper2;

    /**
     * 连接zookeeper server
     */
    public void connect() throws Exception {

        // 创建连接
        log.info("create ZooKeeper begin");
        zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZKConnectSessionWatcher2.ConnWatcher());
        log.info("create ZooKeeper sessionId: " + "0x" + Long.toHexString(zookeeper.getSessionId()));
        log.info("create ZooKeeper end");

        // 等待连接完成
        log.info("create ZooKeeper countDownLatch begin");
        countDownLatch.await();
        log.info("create ZooKeeper sessionId: " + "0x" + Long.toHexString(zookeeper.getSessionId()));
        log.info("create ZooKeeper countDownLatch end");

        log.info("------------------------------------");

        // 开始会话重连
        log.warn("create ZooKeeper2 begin 开始会话重连");
        zookeeper2 = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZKConnectSessionWatcher2.ConnWatcher2(),
                zookeeper.getSessionId(), zookeeper.getSessionPasswd());
        log.info("create ZooKeeper2 sessionId2: " + "0x" + Long.toHexString(zookeeper2.getSessionId()));
        log.info("create ZooKeeper2 end");

        // 等待连接完成
        log.info("create ZooKeeper2 countDownLatch2 begin");
        countDownLatch2.await();
        log.info("create ZooKeeper2 sessionId2: " + "0x" + Long.toHexString(zookeeper2.getSessionId()));
        log.info("create ZooKeeper2 countDownLatch2 end");
    }

    public class ConnWatcher implements Watcher {
        public void process(WatchedEvent event) {
            log.info("ZooKeeper ConnWatcher 接受到watch通知：{}", event);
            // 连接建立, 回调process接口时, 其event.getState()为KeeperState.SyncConnected
            if (event.getState() == Event.KeeperState.SyncConnected) {
                log.info("ZooKeeper ConnWatcher 接受到watch通知：SyncConnected");
                // 放开闸门, wait在connect方法上的线程将被唤醒
                countDownLatch.countDown();
            }
        }
    }

    public class ConnWatcher2 implements Watcher {
        public void process(WatchedEvent event) {
            log.warn("ZooKeeper2 ConnWatcher2 接受到watch通知：{}", event);
            // 连接建立, 回调process接口时, 其event.getState()为KeeperState.SyncConnected
            if (event.getState() == Event.KeeperState.SyncConnected) {
                log.warn("ZooKeeper2 ConnWatcher2 接受到watch通知：SyncConnected");
                // 放开闸门, wait在connect方法上的线程将被唤醒
                countDownLatch2.countDown();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new ZKConnectSessionWatcher2().connect();
    }

}
