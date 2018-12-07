package com.zozospider.zookeepercurator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class CuratorClient {

    private final static Logger log = LoggerFactory.getLogger(CuratorClient.class);

    /**
     * 指定连接的超时时间
     */
    private static final int SESSION_TIMEOUT = 5000;

    private static final int CONNECTION_TIMEOUT = 5000;

    private static final String NAME_SPACE = "namespace";

    public CuratorFramework client;

    private RetryPolicy getRetryPolicy() {
        /**
         * 策略：
         * baseSleepTimeMs: 初始 sleep 的时间
         * maxRetries: 最大重试次数
         * maxSleepMs: 每次重试的最大间隔时间
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * 策略：重试一次
         * sleepMsBetweenRetry: 重试间隔的时间
         */
        RetryPolicy retryPolicy2 = new RetryOneTime(3000);

        /**
         * 策略：重试多次
         * n: 重试次数
         * sleepMsBetweenRetries: 每次重试间隔的时间
         */
        RetryPolicy retryPolicy3 = new RetryNTimes(3, 5000);

        /**
         * 策略：永远重试
         */
        RetryPolicy retryPolicy4 = new RetryForever(5000);

        /**
         * 策略：重试事件超过 maxElapsedTimeMs 后，就不再重试
         * maxElapsedTimeMs: 最大重试时间
         * sleepMsBetweenRetries: 每次重试间隔
         */
        RetryPolicy retryPolicy5 = new RetryUntilElapsed(2000, 3000);

        return retryPolicy3;
    }

    public void connect(String connectString) {
        // 选择一个策略
        RetryPolicy retryPolicy = getRetryPolicy();
        // 创建包含隔离命名空间的会话
        client = CuratorFrameworkFactory.builder()
                .retryPolicy(retryPolicy)
                .connectString(connectString)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .connectionTimeoutMs(CONNECTION_TIMEOUT)
                .namespace(NAME_SPACE)
                .build();
        // 启动客户端
        client.start();
    }

    public void closeClient() {
        if (client != null) {
            this.client.close();
        }
    }

    /**
     * 创建节点
     */
    /**
     * 创建节点
     *
     * @param path 节点路径
     * @param data 节点数据
     * @throws Exception
     */
    public void create(String path, String data) throws Exception {

        // 创建一个节点，初始内容为空
        client.create()
                .forPath(path);
        // 创建一个节点，附带初始化内容
        client.create()
                .forPath(path, data.getBytes());
        // 创建一个节点，指定创建模式（临时节点），内容为空
        client.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path);
        // 创建一个节点，指定创建模式（临时节点），附带初始化内容
        client.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, data.getBytes());
        // 创建一个节点，指定创建模式（临时节点），附带初始化内容，并且自动递归创建父节点
        client.create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, data.getBytes());
        // 创建一个节点，指定创建模式（持久节点），附带初始化内容，并且自动递归创建父节点
        client.create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, data.getBytes());
        // 创建一个节点，指定创建模式（持久节点），附带初始化内容，并且自动递归创建父节点，并且指定 ACL（"world:anyone"）
        client.create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, data.getBytes());
    }

    /**
     * 删除节点
     *
     * @param path    节点路径
     * @param version 当前版本
     * @throws Exception
     */
    public void delete(String path, int version) throws Exception {

        // 删除一个节点
        client.delete()
                .forPath(path);
        // 删除一个节点，并且递归删除其所有的子节点
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(path);
        // 删除一个节点，强制指定版本进行删除
        client.delete()
                .withVersion(version)
                .forPath(path);
        // 删除一个节点，强制保证删除（只要客户端会话有效，那么 Curator 会在后台持续进行删除操作，直到删除节点成功。）
        client.delete()
                .guaranteed()
                .forPath(path);
        // 自由组合条件
        client.delete()
                .guaranteed()
                .deletingChildrenIfNeeded()
                .withVersion(version)
                .forPath(path);
    }

    /**
     * 节点是否存在
     *
     * @param path 节点路径
     * @throws Exception
     */
    public void checkExists(String path) throws Exception {

        // 检查节点是否存在，如果不存在则返回空
        Stat stat = client.checkExists()
                .forPath(path);
    }

    /**
     * 获取节点数据
     *
     * @param path 节点路径
     */
    public void getData(String path) throws Exception {

        // 读取一个节点的数据内容
        byte[] bytes = client.getData()
                .forPath(path);
        // 读取一个节点的数据内容，同时获取到该节点的 Stat
        Stat stat = new Stat();
        byte[] bytes1 = client.getData()
                .storingStatIn(stat)
                .forPath(path);
    }

    /**
     * 获取子节点数据
     *
     * @param path 节点路径
     */
    public void getChildren(String path) throws Exception {

        // 获取某个节点的所有子节点路径，返回子节点 path 列表
        List<String> childPaths = client.getChildren()
                .forPath("path");
    }

    /**
     * 修改节点数据
     *
     * @param path    节点路径
     * @param newData 修改后节点数据
     * @param version 当前版本，用于事务控制（乐观锁，传入错误的版本号将无法执行成功）
     * @throws Exception
     */
    public void setData(String path, String newData, int version) throws Exception {

        // 更新一个节点的数据内容
        client.setData()
                .forPath(path, newData.getBytes());
        // 更新一个节点的数据内容，强制指定版本进行更新
        client.setData()
                .withVersion(version)
                .forPath(path, newData.getBytes());
    }

    /**
     * 事务操作
     *
     * @throws Exception
     */
    public void inTransaction() throws Exception {

        // 可以复合create, setData, check, and/or, delete 等操作然后调用 commit() 作为一个原子操作提交
        client.inTransaction()
                .check()
                .forPath("path")
                .and()
                .create().withMode(CreateMode.EPHEMERAL).forPath("path", "data2".getBytes())
                .and()
                .setData().withVersion(10086).forPath("path", "data2".getBytes())
                .and()
                .commit();
    }

    /**
     * 异步处理
     *
     * @throws Exception
     */
    public void inBackground() throws Exception {

        // 如果不指定 executor，那么会默认使用 Curator 的 EventThread 去进行异步处理
        Executor executor = Executors.newFixedThreadPool(2);

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        log.info("eventType: {}, resultCode: {}", event.getType(), event.getResultCode());
                        log.info("client: {}, event: {}", client, event);
                    }
                }, executor)
                .forPath("path");
    }

    /**
     * 监听（只触发一次）
     *
     * @throws Exception
     */
    public void usingWatcher() throws Exception {

        // 使用 Watcher 监听
        client.getData()
                .usingWatcher(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        log.info("event: {}", event);
                    }
                })
                .forPath("path");

        // 使用 CuratorWatcher 监听
        client.getData()
                .usingWatcher(new CuratorWatcher() {
                    @Override
                    public void process(WatchedEvent event) throws Exception {
                        log.info("event: {}", event);
                    }
                })
                .forPath("path");
    }

    /**
     * 缓存 cache: path cache
     */
    public void pathCache() {

    }

    /**
     * 缓存 cache: node cache
     */
    public void nodeCache() {

    }

    /**
     * 缓存 cache: tree cache
     */
    public void treeCache() {

    }

}
