package com.zozospider.zookeepercurator.configcenter;

import com.alibaba.fastjson.JSON;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * 模拟配置中心客户端逻辑，监听 ZooKeeper 配置对象变更数据
 */
public class Client1 {

    private final static Logger log = LoggerFactory.getLogger(Client1.class);

    public static final String CONNECT_STRING = "123.207.120.205:2181,193.112.38.200:2181,111.230.233.137:2181";
    private static final int CONNECTION_TIMEOUT = 5000;

    public static final String CONFIG_PATH = "/config-center";
    public static final String REDIS_PATH = "/redis-config";
    public static CountDownLatch countDown = new CountDownLatch(1);

    private CuratorFramework client;

    public Client1() {
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STRING)
                .sessionTimeoutMs(CONNECTION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .namespace("namespace")
                .build();
        client.start();
    }

    public void start() throws Exception {
        log.info("启动 Client1");

        // 启动监听
        final PathChildrenCache cache = new PathChildrenCache(client, CONFIG_PATH, true);

        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        cache.getListenable().addListener(new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                // 监听节点变化
                if (PathChildrenCacheEvent.Type.CHILD_UPDATED.equals(event.getType())) {
                    // 获取节点路径
                    String configNodePath = event.getData().getPath();
                    // 判断是否为 redis config 路径
                    if ((CONFIG_PATH + REDIS_PATH).equals(configNodePath)) {
                        log.info("Client1 监听到 redis config 路径数据变化");
                        log.info("路径: {}", configNodePath);

                        // 读取节点 json 字符串
                        String jsonConfig = new String(event.getData().getData());
                        log.info("数据: {}", jsonConfig);

                        // 从 json 字符串转为对象
                        RedisConfig redisConfig = JSON.parseObject(jsonConfig, RedisConfig.class);

                        if (redisConfig != null) {

                            if ("add".equals(redisConfig.getType())) {

                                // 客户端执行添加操作
                                log.info("监听到新增配置，准备下载...");
                                // 连接 ftp 服务器，根据 url 找到相应的配置
                                Thread.sleep(500);
                                log.info("开始下载新的配置文件，id: {}, url: {}", redisConfig.getId(), redisConfig.getUrl());
                                // 执行下载逻辑
                                Thread.sleep(500);
                                log.info("完成下载");
                                // 客户端执行添加逻辑
                                Thread.sleep(500);
                                log.info("Client1 完成新增");

                            } else if ("update".equals(redisConfig.getType())) {

                                // 客户端执行更新操作
                                log.info("监听到更新配置，准备下载...");
                                // 连接 ftp 服务器，根据 url 找到相应的配置
                                Thread.sleep(500);
                                log.info("开始下载新的配置文件，id: {}, url: {}", redisConfig.getId(), redisConfig.getUrl());
                                // 执行下载逻辑
                                Thread.sleep(500);
                                log.info("完成下载");
                                // 客户端执行更新逻辑
                                Thread.sleep(500);
                                log.info("Client1 完成更新");

                            } else if ("delete".equals(redisConfig.getType())) {

                                // 客户端执行删除操作
                                log.info("监听到删除配置");
                                log.info("删除配置文件，id: {}", redisConfig.getId());
                                // 客户端执行删除逻辑
                                Thread.sleep(500);
                                log.info("Client1 完成删除");
                            }
                        }
                    }
                }
            }
        });

        // 进程挂起
        countDown.await();
    }

}
