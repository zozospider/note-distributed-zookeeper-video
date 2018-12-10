package com.zozospider.zookeepercurator.leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Leader 选举过程：在任务开始前，哪个节点都不知道谁是 leader 或 coordinator。当选举算法开始执行后，最终会产生一个唯一节点作为 leader。
 * <p>
 * leaderLatch.start() 一旦启动，leaderLatch 会和其他 leaderLatch 交涉，选出一个作为 leader，此过程会阻塞。
 */
public class LeaderLatchClient {

    private final static Logger log = LoggerFactory.getLogger(LeaderLatchClient.class);

    private static final String PATH = "/leader";
    private static final int CLIENT_QTY = 10;

    public static void main(String[] args) throws Exception {

        // 模拟多个客户端
        List<CuratorFramework> clients = Lists.newArrayList();
        // 多个 LeaderLatch 实例，为多个客户端提供选举
        List<LeaderLatch> latches = Lists.newArrayList();
        // 模拟服务端
        TestingServer server = new TestingServer();
        try {

            // 模拟多个客户端参与选举逻辑
            for (int i = 0; i < CLIENT_QTY; i++) {

                // 新建客户端连接
                CuratorFramework client = CuratorFrameworkFactory.newClient(
                        server.getConnectString(), new RetryNTimes(3, 5000));

                // 创建 LeaderLatch 选举对象
                LeaderLatch latch = new LeaderLatch(client, PATH, "Client #" + i);

                // LeaderLatch 监听是否被选举为 Leader
                latch.addListener(new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        log.info("I am leader");
                    }

                    @Override
                    public void notLeader() {
                        log.info("I am not leader");
                    }
                });

                // 添加到集合（用于 finally 关闭连接）
                clients.add(client);
                latches.add(latch);

                // 启动客户端并加入选举
                client.start();
                latch.start();
                log.info("client {} start, LeaderLatch start", i);
            }

            // 等待选举结果
            Thread.sleep(20000);

            // 遍历筛选出被选为 leader 的选举对象
            LeaderLatch currentLeader = null;
            for (LeaderLatch latch : latches) {
                if (latch.hasLeadership()) {
                    currentLeader = latch;
                }
            }
            log.info("current leader id: {}", currentLeader.getId());

            // 模拟释放当前 leader，其他客户端重新选举出新 leader
            currentLeader.close();
            log.info("current leader id: {} closed", currentLeader.getId());

            // 再次等待选举结果
            Thread.sleep(20000);
            for (LeaderLatch latch : latches) {
                if (latch.hasLeadership()) {
                    currentLeader = latch;
                }
            }
            log.info("current new leader id: {}", currentLeader.getId());

            log.info("close LeaderLatch and CuratorFramework");
        } finally {
            // 关闭 LeaderLatch 选举对象和 CuratorFramework 客户端
            for (LeaderLatch latch : latches) {
                CloseableUtils.closeQuietly(latch);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
        }
    }

}
