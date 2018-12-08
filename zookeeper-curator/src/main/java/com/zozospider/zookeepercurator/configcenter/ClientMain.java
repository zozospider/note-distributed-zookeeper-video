package com.zozospider.zookeepercurator.configcenter;

public class ClientMain {

    public static void main(String[] args) throws Exception {

        // 启动三个客户端，对 ZooKeeper 节点数据的变化进行监听
        new Client1().start();
//        new Client2().start();
//        new Client3().start();
    }

}
