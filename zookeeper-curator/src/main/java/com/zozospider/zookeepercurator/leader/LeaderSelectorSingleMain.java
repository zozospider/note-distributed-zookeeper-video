package com.zozospider.zookeepercurator.leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 类似 LeaderLatch，所有存活的客户端不间断的轮流做 Leader。
 * <p>
 * 参考 LeaderSelectorMultiMain
 */
public class LeaderSelectorSingleMain {

    private final static Logger log = LoggerFactory.getLogger(LeaderSelectorSingleMain.class);

    private static final String PATH = "/leader/selector/single";
    private static final int CLIENT_QTY = 5;

    public static void main(String[] args) throws Exception {

        // 模拟多个客户端
        List<CuratorFramework> clients = Lists.newArrayList();

        // 多个 LeaderSelector 选举对象
        List<LeaderSelector> selectors = Lists.newArrayList();

        // 模拟服务端
        TestingServer server = new TestingServer();

        try {

            // 模拟多个客户端参与选举逻辑
            for (int i = 0; i < CLIENT_QTY; i++) {

                // 新建客户端连接
                CuratorFramework client = CuratorFrameworkFactory.newClient(
                        server.getConnectString(), new RetryNTimes(3, 5000));

                // 创建 LeaderSelector 选举对象
                LeaderSelector selector = new LeaderSelector(client, PATH, new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {

                        log.info("I am leader");

                        // 随机等待时间（在此等待期间，该客户端为 Leader，方法结束后失去 Leader）
                        final int waitSeconds = (int) (5 * Math.random()) + 1;
                        log.info("current Client Waiting {} seconds ...", waitSeconds);
                        try {
                            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
                        } catch (InterruptedException e) {
                            log.info("Client was interrupted");
                            Thread.currentThread().interrupt();
                        } finally {
                            // 该方法执行结束，即放弃 Leader，重新选举。
                            // 如果你想要要此实例一直是leader的话可以加一个死循环。
                            log.info("current Client: relinquishing leadership");
                        }
                    }
                });

                // 添加到集合（用于 finally 关闭连接）
                clients.add(client);
                selectors.add(selector);

                // 启动客户端并加入选举
                client.start();
                selector.start();

                log.info("client {} start, LeaderSelector start", i);
            }

            // 关闭
            log.info("Press enter return to quit");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            log.info("close LeaderSelector and CuratorFramework");

        } finally {
            // 关闭 LeaderSelector 选举对象和 CuratorFramework 客户端
            for (LeaderSelector selector : selectors) {
                CloseableUtils.closeQuietly(selector);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            // 关闭模拟服务端
            CloseableUtils.closeQuietly(server);
        }
    }

}
