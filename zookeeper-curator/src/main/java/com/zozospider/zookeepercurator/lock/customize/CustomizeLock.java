package com.zozospider.zookeepercurator.lock.customize;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 自定义分布式锁（未成功实现）
 */
public class CustomizeLock {

    private final static Logger log = LoggerFactory.getLogger(CustomizeLock.class);

    // 分布式锁的总节点名
    private static final String LOCK_PATH = "/customize-locks";

    // ZooKeeper 客户端
    private CuratorFramework client;

    // 当前锁路径
    private String path;

    // 用于挂起当前请求，并且等待上一个分布式锁释放
    private CountDownLatch countDown = new CountDownLatch(1);

    public CustomizeLock(CuratorFramework client, String path) throws Exception {
        this.client = client;
        this.path = path;
        // 创建节点，添加监听
        init();
    }

    /**
     * 创建节点，添加监听
     *
     * @throws Exception
     */
    public void init() throws Exception {
        log.info("stat begin");

        final int waitSeconds = (int) (5 * Math.random()) + 1;
        Thread.sleep(TimeUnit.SECONDS.toMillis(new Random().nextInt(waitSeconds)));

        Stat stat = client.checkExists().forPath(LOCK_PATH);
        if (stat == null) {
            log.info("create begin");
            // 创建节点
            try {
                String s = client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(LOCK_PATH);
            } catch (Exception e) {
                log.error("create error: e: " + e.getMessage(), e);
                throw new Exception("create error");
            }
            log.info("create end");
        }
        log.info("stat end");
        log.info("addWatch begin");
        // 添加监听
        addWatch();
        log.info("addWatch end");
    }

    public void addWatch() throws Exception {

        // 子节点监听对象
        final PathChildrenCache cache = new PathChildrenCache(client, LOCK_PATH, true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        // 添加监听
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {

                // 子节点被移除
                if (PathChildrenCacheEvent.Type.CHILD_REMOVED.equals(event.getType())) {
                    String currentPath = event.getData().getPath();
                    log.info("上一个会话已释放锁或该会话已断开, 节点路径为: " + currentPath);
                    if (currentPath.equals(LOCK_PATH + path)) {
                        log.info("释放计数器, 让当前请求来获得分布式锁 ...");
                        countDown.countDown();
                    }
                }

            }
        });
    }

    public void acquire() {

        // 使用死循环，当且仅当上一个锁释放并且当前请求获得锁成功后才会跳出
        while (true) {
            try {
                // 创建节点
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(LOCK_PATH + path);
                log.info("获得分布式锁成功 ...");
                return;

            } catch (Exception e) {

                log.info("获得分布式锁失败 ...");
                try {
                    // 如果没有获取到锁，需要重新设置同步资源值
                    if (countDown.getCount() == 0) {
                        countDown = new CountDownLatch(1);
                    }
                    // 阻塞线程
                    countDown.await();

                } catch (Exception e1) {
                    log.error("getLock, countDown.getCount(), new CountDownLatch(), countDown.await() exception, e: " + e.getMessage(),
                            e);
                }

            }
        }
    }

    public void acquire2() throws Exception {

        // 使用死循环，当且仅当上一个锁释放并且当前请求获得锁成功后才会跳出
        while (true) {
            // 判断节点是否存在
            Stat stat = client.checkExists().forPath(LOCK_PATH + path);

            // 节点不存在
            if (stat == null) {
                // 创建节点
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(LOCK_PATH + path);
                log.info("获得分布式锁成功 ...");
                return;

            } else {

                log.info("获得分布式锁失败 ...");
                // 节点存在，表示没有获取到锁，需要重新设置同步资源值
                if (countDown.getCount() == 0) {
                    countDown = new CountDownLatch(1);
                }
                // 阻塞线程
                countDown.await();
            }
        }

    }

    public boolean release() throws Exception {

        try {
            Stat stat = client.checkExists().forPath(LOCK_PATH + path);
            // 节点存在
            if (stat != null) {
                // 删除节点
                client.delete().forPath(LOCK_PATH + path);
            }
            log.info("分布式锁释放完毕");
            return true;

        } catch (Exception e) {
            log.error("releaseLock exception, e: " + e.getMessage(), e);
            return false;
        }
    }

}
