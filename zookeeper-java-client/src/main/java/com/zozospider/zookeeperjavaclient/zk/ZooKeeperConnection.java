package com.zozospider.zookeeperjavaclient.zk;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ZooKeeperConnection {

    final static Logger log = LoggerFactory.getLogger(ZooKeeperConnection.class);

    /**
     * ZooKeeper server列表，以逗号隔开。
     * ZooKeeper 对象初始化后，将从 server 列表中选择一个 server，并尝试与其建立连接。
     * 如果连接建立失败，则会从列表的剩余项中选择一个 server，并再次尝试建立连接。
     */
    public static final String connectString = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";
    /**
     * 指定连接的超时时间
     */
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ZooKeeper zookeeper;



}
