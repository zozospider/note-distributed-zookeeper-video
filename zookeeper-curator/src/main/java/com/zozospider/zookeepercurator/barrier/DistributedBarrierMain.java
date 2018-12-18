package com.zozospider.zookeepercurator.barrier;

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
 * 分布式屏障（DistributedBarrier）
 * <p>
 * 它会阻塞所有节点上的等待进程（等待 waitOnBarrier()），直到某一个被满足（移除栅栏 removeBarrier()），然后所有的节点继续进行。
 */
public class DistributedBarrierMain {

    private final static Logger log = LoggerFactory.getLogger(DistributedBarrierMain.class);

    private static final String PATH = "/counter/DistributedBarrier";
    private static final int CLIENT_QTY = 5;

    public static void main(String[] args) throws Exception {

        // 模拟服务端
        final TestingServer server = new TestingServer();

        // 生成 5 个线程服务
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_QTY);
        try {

            // 新建 5 个异步任务（线程），模拟多个客户端参与计数逻辑
            // 每个任务（线程）新建 1 个 Operator，包含 1 个 DistributedBarrier
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
                            log.info("execute task a, Ca{}", ii);

                            // 新建 1 个 Operator，包含 1 个 DistributedBarrier
                            DistributedBarrierOperator operator =
                                    new DistributedBarrierOperator(client, PATH, "Ca" + ii);

                            // 设置栅栏
                            log.info("Client Ca{} setBarrier begin", ii);
                            operator.setBarrier();
                            log.info("Client Ca{} setBarrier end", ii);

                            Thread.sleep(1000 * new Random().nextInt(5));

                            // 等待（此方法执行后会一直阻塞，直到其他客户端移除栅栏）
                            log.info("Client Ca{} waitOnBarrier begin", ii);
                            operator.waitOnBarrier();
                            log.info("Client Ca{} waitOnBarrier end", ii);

                        } finally {
                            CloseableUtils.closeQuietly(client);
                        }

                        return null;
                    }
                };
                log.info("submit task a, Ca{}" + ii);
                // 提交异步任务
                service.submit(task);
            }

            // 以上所有客户端，设置了栅栏，并等待。
            Thread.sleep(20000);
            log.info("above Barrier instances Ca{} to Ca{} should wait on barrier ...", 0, CLIENT_QTY);

            // 以下一个客户端，移除栅栏，移除后，所有的线程继续执行。
            log.info("below Barrier instances Cb0 is going to remove barrier ...");
            // 新建客户端连接
            CuratorFramework client = CuratorFrameworkFactory.newClient(
                    server.getConnectString(), new RetryNTimes(3, 5000));
            try {
                // 启动客户端
                client.start();
                // 新建 1 个 Operator，包含 1 个 DistributedBarrier
                DistributedBarrierOperator operator = new DistributedBarrierOperator(client, PATH, "Cb0");
                // 移除栅栏
                log.info("Client Cb0 removeBarrier begin");
                operator.removeBarrier();
                log.info("Client Cb0 removeBarrier end");
            } finally {
                CloseableUtils.closeQuietly(client);
            }

            // 等待一段时间，观察设置栅栏的客户端回调
            Thread.sleep(8000);
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        } finally {
            CloseableUtils.closeQuietly(server);
        }

    }

}
