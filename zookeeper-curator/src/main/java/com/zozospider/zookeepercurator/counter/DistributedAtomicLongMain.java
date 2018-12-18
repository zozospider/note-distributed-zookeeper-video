package com.zozospider.zookeepercurator.counter;

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
 * 分布式 long 计数器（DistributedAtomicLong）
 * 1. 计数的范围比 SharedCount 大。
 * 2. 它首先尝试使用乐观锁的方式设置计数器，如果不成功（比如期间计数器已经被其它client更新了），则使用 InterProcessMutex 方式来更新计数值。
 * <p>
 * 参考: SharedCountMain.java
 */
public class DistributedAtomicLongMain {

    private final static Logger log = LoggerFactory.getLogger(DistributedAtomicLongMain.class);

    private static final String PATH = "/counter/DistributedAtomicLong";
    private static final int CLIENT_QTY = 5;

    public static void main(String[] args) throws Exception {

        // 模拟服务端
        final TestingServer server = new TestingServer();

        // 生成 5 个线程服务
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_QTY);
        try {

            // 新建 5 个异步任务（线程），模拟多个客户端参与计数逻辑
            // 每个任务（线程）新建 1 个 Operator，包含 1 个 DistributedAtomicLong
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
                            log.info("execute task, C{}", ii);

                            // 新建 1 个 Operator，包含 1 个 DistributedAtomicLong
                            DistributedAtomicLongOperator operator = new DistributedAtomicLongOperator(client, PATH, "C" + ii);

                            // 加一
                            operator.increment();

                            // 减一
//                            operator.decrement();

                            // 增加特定的值
//                            operator.add(new Random().nextInt(10));

                            // 减去特定的值
//                            operator.subtract(new Random().nextInt(10));

                            // 尝试更新（注意，此更新可能不成功）
//                            operator.trySet(new Random().nextInt(10));

                            // 强制更新
//                            operator.forceSet(new Random().nextInt(10));

                            // 获取当前值
                            operator.get();

                        } finally {
                            CloseableUtils.closeQuietly(client);
                        }

                        return null;
                    }
                };

                log.info("submit task, C{}" + ii);
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
