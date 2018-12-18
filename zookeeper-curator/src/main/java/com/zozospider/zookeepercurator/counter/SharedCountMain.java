package com.zozospider.zookeepercurator.counter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 分布式 int 计数器（SharedCount）
 * 任意的 SharedCount， 只要使用相同的 path，都可以得到这个计数值。
 */
public class SharedCountMain {

    private final static Logger log = LoggerFactory.getLogger(SharedCountMain.class);

    private static final String PATH = "/counter/sharedCount";
    private static final int CLIENT_QTY = 5;

    public static void main(String[] args) throws Exception {

        // 模拟服务端
        final TestingServer server = new TestingServer();

        // 生成 5 个线程服务
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_QTY);
        try {

            // 新建 3 个异步任务（线程），模拟多个客户端参与计数逻辑
            // 每个任务（线程）新建 1 个 Adapter，包含 1 个 SharedCount
            for (int i = 0; i < CLIENT_QTY; i++) {
                final int ii = i;

                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {

                        // 新建客户端连接
                        CuratorFramework client = CuratorFrameworkFactory.newClient(
                                server.getConnectString(), new RetryNTimes(3, 5000));

                        // 创建 SharedCountAdapter 计数器对象适配器（也可使用匿名对象实现）
                        SharedCountAdapter adapter = new SharedCountAdapter(client, PATH, "C" + ii);
                        try {
                            // 启动客户端和适配器
                            client.start();
                            adapter.start();
                            log.info("execute task, C{}", ii);

                            // 随机等待一段时间再执行，减少多个线程并发对计数器进行操作导致尝试新增失败的情况（注意，无法确保一定能更新成功）
                            Thread.sleep(new Random().nextInt(10000));

                            // 在原有基础上尝试新增（注意，如上所示，有可能更新失败，因为并没有加锁控制多线程的并发更新问题）
                            adapter.trySetCount(new Random().nextInt(10));

                            // 等待一段时间再关闭 client 和 adapter，用于 SharedCountListener 监听
                            Thread.sleep(new Random().nextInt(5000));
                        } finally {
                            CloseableUtils.closeQuietly(client);
                            CloseableUtils.closeQuietly(adapter);
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
