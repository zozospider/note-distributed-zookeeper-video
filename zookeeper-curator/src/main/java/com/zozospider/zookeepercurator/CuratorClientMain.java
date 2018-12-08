package com.zozospider.zookeepercurator;

public class CuratorClientMain {

    public static final String CONNECT_STRING = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";

    public static void main(String[] args) {

        CuratorClient curatorClient = new CuratorClient();

        curatorClient.connect(CONNECT_STRING);
    }

}
