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
 * 分布式屏障双栅栏（DistributedDoubleBarrier）
 * <p>
 * 所有客户端都调用 enter() 方法到 memberQty（初始化时指定参数） 次后，所有客户端进程才会继续往下执行。（就像百米赛跑比赛，所有运动员都到达起点，开始发令枪响，运动员才能开始跑）
 * 所有客户端都调用 leave() 方法到 memberQty（初始化时指定参数） 次后，所有客户端进程才会继续往下执行。（就像百米赛跑比赛，所有运动员都到达终点，结束发令枪响，运动员才能退出赛场）
 */
public class DistributedDoubleBarrierMain {

    private final static Logger log = LoggerFactory.getLogger(DistributedDoubleBarrierMain.class);

    private static final String PATH = "/counter/DistributedDoubleBarrier";
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
                            log.info("execute task, C{}", ii);

                            // 新建 1 个 Operator，包含 1 个 DistributedDoubleBarrier
                            DistributedDoubleBarrierOperator operator =
                                    new DistributedDoubleBarrierOperator(client, PATH, CLIENT_QTY, "C" + ii);

                            // 等待一段时间，模拟不同客户端调用 enter() 时间不一致
                            Thread.sleep(1000 * new Random().nextInt(20));

                            // 进入（此方法会阻塞，直到所有客户端都调用结束）
                            log.info("Client C{} enter begin", ii);
                            operator.enter();
                            log.info("Client C{} enter end", ii);


                            // 等待一段时间，模拟不同客户端调用 leave() 时间不一致
                            Thread.sleep(1000 * new Random().nextInt(20));

                            // 离开（此方法也会阻塞，直到所有客户端都调用结束）
                            log.info("Client C{} leave begin", ii);
                            operator.leave();
                            log.info("Client C{} leave end", ii);

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
