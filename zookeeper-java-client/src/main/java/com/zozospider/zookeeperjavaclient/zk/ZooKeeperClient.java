package com.zozospider.zookeeperjavaclient.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper Java client
 */
public class ZooKeeperClient {

    final static Logger log = LoggerFactory.getLogger(ZooKeeperClient.class);

    /**
     * 指定连接的超时时间
     */
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch;
    private ZooKeeper zookeeper;

    /**
     * 实现对 ZooKeeper 事件监听
     */
    public class ConnWatcher implements Watcher {

        public void process(WatchedEvent event) {

            log.info("ConnWatcher process watchedEvent: {}", event);

            if (event.getState() == Event.KeeperState.SyncConnected) {
                // 连接建立，回调 process 接口时，其 event.getState() 为 KeeperState.SyncConnected
                log.info("ConnWatcher process watchedEvent state: SyncConnected");
                // 放开闸门，wait 在 connect 方法上的线程将被唤醒
                countDownLatch.countDown();
            }

            if (event.getType() == Event.EventType.NodeCreated) {
                log.info("ConnWatcher process watchedEvent type: NodeCreated");
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                log.info("ConnWatcher process watchedEvent type: NodeDataChanged");
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                log.info("ConnWatcher process watchedEvent type: NodeDeleted");
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                log.info("ConnWatcher process watchedEvent type: NodeChildrenChanged");
            }
        }

    }

    /**
     * 与 ZooKeeper 服务端建立连接（同步）
     *
     * @param connectString ZooKeeper server列表，以逗号隔开。
     *                      ZooKeeper 对象初始化后，将从 server 列表中选择一个 server，并尝试与其建立连接。
     *                      如果连接建立失败，则会从列表的剩余项中选择一个 server，并再次尝试建立连接。
     * @throws Exception
     */
    public void connect(String connectString) throws Exception {

        // 创建连接
        log.info("connect ZooKeeper begin");
        zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZooKeeperClient.ConnWatcher());
        log.info("create ZooKeeper sessionId: 0x{}", Long.toHexString(zookeeper.getSessionId()));
        log.info("connect ZooKeeper end");

        // 等待连接完成
        log.info("connect ZooKeeper countDownLatch begin");
        countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
        log.info("connect ZooKeeper sessionId: 0x{}", Long.toHexString(zookeeper.getSessionId()));
        log.info("connect ZooKeeper countDownLatch end");
    }

    /**
     * 会话重连（同步）
     *
     * @param connectString 同上
     * @throws IOException
     * @throws InterruptedException
     */
    public void connectSession(String connectString) throws IOException, InterruptedException {

        // 开始会话重连
        log.warn("connectSession ZooKeeper begin 开始会话重连");
        // sessionId:      已建立会话的 session id
        // sessionPasswd:  已建立会话的 session password
        zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, new ZooKeeperClient.ConnWatcher(),
                zookeeper.getSessionId(), zookeeper.getSessionPasswd());
        log.info("connectSession ZooKeeper sessionId: 0x{}", Long.toHexString(zookeeper.getSessionId()));
        log.info("connectSession ZooKeeper end");

        // 等待连接完成
        log.info("connectSession ZooKeeper countDownLatch begin");
        countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
        log.info("connectSession ZooKeeper sessionId: " + "0x" + Long.toHexString(zookeeper.getSessionId()));
        log.info("connectSession ZooKeeper countDownLatch end");
    }

    /**
     * 创建持久节点
     *
     * @param path  节点路径
     * @param data  节点数据
     * @param async 是否异步
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String path, String data, boolean async) throws KeeperException, InterruptedException {

        log.info("create begin");
        // ZooDefs.Ids.OPEN_ACL_UNSAFE:  world:anyone:cdrwa
        // CreateMode.PERSISTENT:        创建持久节点
        if (!async) {
            // 同步
            String str = zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("create sync str: {}", str);
        } else {
            // 异步
            zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    // do something when callback
                    if (rc == KeeperException.Code.OK.intValue()) {
                        log.info("create async success");
                    } else {
                        KeeperException.Code code = KeeperException.Code.get(rc);
                        log.error("create async failure, rc codeValue: {}, rc codeName: {}", code.intValue(), code.name());
                    }
                    log.info("create async rc: {}, path: {}, ctx: {}, name: {}", rc, path, ctx, name);
                }
            }, "create async Object ctx");
            // 仅测试（监听回调，避免主线程中止）
            Thread.sleep(3000);
        }
        log.info("create end");
    }

    /**
     * 删除节点
     *
     * @param path    节点路径
     * @param version 当前版本，用于事务控制（乐观锁，传入错误的版本号将无法执行成功）
     * @param async   是否异步
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void delete(String path, int version, boolean async) throws KeeperException, InterruptedException {

        log.info("delete begin");
        if (!async) {
            // 同步
            zookeeper.delete(path, version);
        } else {
            // 异步
            zookeeper.delete(path, version, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    // do something when callback
                    if (rc == KeeperException.Code.OK.intValue()) {
                        log.info("delete async success");
                    } else {
                        KeeperException.Code code = KeeperException.Code.get(rc);
                        log.error("delete async failure, rc codeValue: {}, rc codeName: {}", code.intValue(), code.name());
                    }
                    log.info("delete async rc: {}, path: {}, ctx: {}", rc, path, ctx);
                }
            }, "delete async Object ctx");
            // 仅测试（监听回调，避免主线程中止）
            Thread.sleep(3000);
        }
        log.info("delete end");
    }

    /**
     * 节点是否存在
     *
     * @param path 节点路径
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {

        log.info("exists begin");
        Stat stat = zookeeper.exists(path, false);
        log.info("exists stat: {}", stat);
        boolean exists = !(stat == null);
        log.info("exists ? {} : {}", path, exists);
        log.info("exists end");
        return exists;
    }

    /**
     * 获取节点数据
     *
     * @param path 节点路径
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getData(String path) throws KeeperException, InterruptedException {

        log.info("getData begin");
        Stat stat = new Stat();
        log.info("getData begin stat: {}", stat);
        byte[] data = zookeeper.getData(path, false, stat);
        String result = new String(data);
        log.info("getData result: {}", result);
        log.info("getData end stat: {}", stat);
        log.info("getData end");
        return result;
    }

    /**
     * 获取子节点数据
     *
     * @param path  节点路径
     * @param async 是否异步
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getChildren(String path, boolean async) throws KeeperException, InterruptedException {

        log.info("getChildren begin");
        if (!async) {
            // 同步
            List<String> children = zookeeper.getChildren(path, true);
            log.info("getChildren sync children: {}", children);
        } else {
            zookeeper.getChildren(path, true, new AsyncCallback.ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    // do something when callback
                    if (rc == KeeperException.Code.OK.intValue()) {
                        log.info("getChildren async success");
                    } else {
                        KeeperException.Code code = KeeperException.Code.get(rc);
                        log.error("getChildren async failure, rc codeValue: {}, rc codeName: {}", code.intValue(), code.name());
                    }
                    log.info("getChildren async rc: {}, path: {}, ctx: {}, children: {}", rc, path, ctx, children);
                }
            }, "getChildren async Object ctx");
            // 仅测试（监听回调，避免主线程中止）
            Thread.sleep(3000);
        }
        log.info("getChildren end");
    }

    /**
     * 修改节点数据
     *
     * @param path    节点路径
     * @param newData 修改后节点数据
     * @param version 当前版本，用于事务控制（乐观锁，传入错误的版本号将无法执行成功）
     * @param async   是否异步
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void setData(String path, String newData, int version, boolean async) throws KeeperException, InterruptedException {

        log.info("setData begin");
        if (!async) {
            // 同步
            Stat stat = zookeeper.setData(path, newData.getBytes(), version);
            log.info("setData sync stat: {}", stat);
        } else {
            // 异步
            zookeeper.setData(path, newData.getBytes(), version, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    // do something when callback
                    if (rc == KeeperException.Code.OK.intValue()) {
                        log.info("setData async success");
                    } else {
                        KeeperException.Code code = KeeperException.Code.get(rc);
                        log.error("setData async failure, rc codeValue: {}, rc codeName: {}", code.intValue(), code.name());
                    }
                    log.info("setData async rc: {}, path: {}, ctx: {}, stat: {}", rc, path, ctx, stat);
                }
            }, "setData async Object ctx");
            // 仅测试（监听回调，避免主线程中止）
            Thread.sleep(3000);
        }
        log.info("setData end");
    }

}
