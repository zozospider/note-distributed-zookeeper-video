package com.zozospider.zookeepercurator.leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * LeaderSelector 选举第二版
 * <p>
 * 参考 LeaderSelectorSingle
 */
public class LeaderSelectorClient {

    private final static Logger log = LoggerFactory.getLogger(LeaderSelectorClient.class);

    protected static String PATH = "/leader/selector";
    private static final int CLIENT_QTY = 5;

    public static void main(String[] args) throws Exception {

        // 模拟多个客户端
        List<CuratorFramework> clients = Lists.newArrayList();
        // 多个 LeaderSelectorAdapter 选举对象适配器
        List<LeaderSelectorAdapter> adapters = Lists.newArrayList();
        // 模拟服务端
        TestingServer server = new TestingServer();
        try {

            // 模拟多个客户端参与选举逻辑
            for (int i = 0; i < CLIENT_QTY; i++) {

                // 新建客户端连接
                CuratorFramework client = CuratorFrameworkFactory.newClient(
                        server.getConnectString(), new RetryNTimes(3, 5000));

                // 创建 LeaderSelectorAdapter 选举对象适配器（也可使用匿名对象实现）
                LeaderSelectorAdapter adapter = new LeaderSelectorAdapter(client, PATH, "Client #" + i);

                // 添加到集合（用于 finally 关闭连接）
                clients.add(client);
                adapters.add(adapter);

                // 启动客户端并加入选举
                client.start();
                adapter.start();

                log.info("client {} start, LeaderSelectorAdapter start", i);
            }

            // 关闭
            log.info("Press enter return to quit");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            log.info("close LeaderSelectorAdapter and CuratorFramework");

        } finally {
            // 关闭 LeaderSelectorAdapter 选举对象和 CuratorFramework 客户端
            for (LeaderSelectorAdapter adapter : adapters) {
                CloseableUtils.closeQuietly(adapter);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            // 关闭模拟服务端
            CloseableUtils.closeQuietly(server);
        }

    }

}
