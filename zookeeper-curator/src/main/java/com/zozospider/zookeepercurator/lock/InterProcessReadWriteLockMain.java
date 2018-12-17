package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 模拟多个客户端多个线程调用：可重入读写锁（Shared Reentrant Read Write Lock）
 */
public class InterProcessReadWriteLockMain {

    private final static Logger log = LoggerFactory.getLogger(InterProcessReadWriteLockMain.class);

    // 需要加锁的路径
    private static final String LOCK_PATH = "/lock/InterProcessReadWriteLock";
    // 客户端数
    private static final int CLIENT_QTY = 3;
    // 每个客户端调用 operator.doLock() 次数
    private static final int DO_TIMES = 5;

    public static void main(String[] args) throws Exception {

        // 共享资源对象
        final LimitedResource resource = new LimitedResource();

        // 生成 5 个线程服务
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_QTY);

        // 模拟服务端
        final TestingServer server = new TestingServer();

        try {

            // 新建 3 个异步任务（线程）
            // 每个任务（线程）新建 1 个 Operator，包含 1 个 lock
            // 每个任务（线程）调用 5 次 operator.doLock()
            for (int i = 0; i < CLIENT_QTY; i++) {
                final int ii = i;

                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {

                        // 新建客户端连接
                        CuratorFramework client = CuratorFrameworkFactory.newClient(
                                server.getConnectString(), new RetryNTimes(3, 5000));
                        try {
                            // 启动客户端
                            client.start();
                            log.info("execute task, Client: {}" + ii);

                            // 新建 1 个 Operator，包含 1 个 lock
                            final InterProcessReadWriteLockOperator operator = new InterProcessReadWriteLockOperator(resource, "C" + ii,
                                    client, LOCK_PATH);

                            // 每个任务（线程）调用 5 次 operator.doLock()
                            for (int j = 0; j < DO_TIMES; j++) {

                                // 获取一次读锁并访问共享资源对象，完成后释放一次读锁（预计可顺利完成所有线程逻辑）
                                operator.doWriteLock(j);

                                // 获取一次读锁，一次写锁，并访问共享资源对象，完成后释放一次读锁，一次写锁（预计可顺利完成所有线程逻辑）
//                                operator.doWriteReadLock(j);

                                // 获取一次读锁，两次写锁，并访问共享资源对象，完成后释放一次读锁，两次写锁（预计可顺利完成所有线程逻辑）
//                                operator.doWriteReadLockTwice(j);
                            }

                        } finally {
                            CloseableUtils.closeQuietly(client);
                        }

                        return null;
                    }
                };

                log.info("submit task, Client: {}" + ii);
                // 提交异步任务
                service.submit(task);

            }

            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);

        } finally {
            CloseableUtils.closeQuietly(server);
        }

    }

}