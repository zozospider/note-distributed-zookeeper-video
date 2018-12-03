package com.zozospider.zookeeperjavaclient.zk;

public class ZooKeeperConnectionMain {

    public static final String CONNECT_STRING = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";

    public static void main(String[] args) throws Exception {

        ZooKeeperClient zooKeeperClient = new ZooKeeperClient();

        // 建立连接（同步）
        zooKeeperClient.connect(CONNECT_STRING);

        // 会话重连（同步）
//        zooKeeperClient.connectSession(CONNECT_STRING);

        // 获取子节点（同步）
        zooKeeperClient.getChildren("/", false);

        // 获取子节点（异步）
        zooKeeperClient.getChildren("/", true);

        // 创建持久节点（同步）
//        zooKeeperClient.create("/zkCli", "zkCli data", false);

        // 创建持久节点（异步）
//        zooKeeperClient.create("/zkCli/zz", "zz data", true);

        // 创建持久节点（异步）
//        zooKeeperClient.create("/zkCli2", "zkCli2 data", true);

        // 节点是否存在
        zooKeeperClient.exists("/zkCli");
        zooKeeperClient.exists("/zkCli/zz");
        zooKeeperClient.exists("/zkCliNo");

        // 获取节点数据
//        zooKeeperClient.getData("/zkCli");

        // 修改节点数据（同步）
//        zooKeeperClient.setData("/zkCli", "zkCli newData", 0, false);

        // 修改节点数据（异步）
//        zooKeeperClient.setData("/zkCli", "zkCli newData 2", 1, true);

        // 获取节点数据
//        zooKeeperClient.getData("/zkCli");

        // 删除节点（同步）
//        zooKeeperClient.delete("/zkCli", 0, false);

        // 删除节点（同步）
//        zooKeeperClient.delete("/zkCli2", 2, true);

    }

}
