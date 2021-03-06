package com.zozospider.zookeepercurator.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LeaderSelector 适配器类
 * <p>
 * LeaderSelector 为其成员变量。同时实现了 new LeaderSelector() 构造方法的参数 LeaderSelectorListener 类。此 Listener 可以监听到选举的事件。
 */
public class LeaderSelectorAdapter extends LeaderSelectorListenerAdapter implements Closeable {

    private final static Logger log = LoggerFactory.getLogger(LeaderSelectorAdapter.class);

    // 客户端名称
    private final String name;
    // 真正参与选举的 LeaderSelector 对象
    private final LeaderSelector leaderSelector;
    // 统计
    private final AtomicInteger counter = new AtomicInteger();

    public LeaderSelectorAdapter(CuratorFramework client, String path, String name) {
        this.name = name;
        // 创建 LeaderSelector 对象，并实现自我监听
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start() {
        leaderSelector.start();
    }

    /**
     * 被选举为 Leader 时触发，方法结束时会立即释放 Leader，重新选举
     *
     * @param client 客户端
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) {

        log.info("current Client: {} is now the leader, It has been leader {} time(s) before ...", name, counter.getAndIncrement());

        // 随机等待时间（在此等待期间，该客户端为 Leader，方法结束后失去 Leader）
        final int waitSeconds = (int) (5 * Math.random()) + 1;
        log.info("current Client: {} Waiting {} seconds ...", name, waitSeconds);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            log.info("Client: {} was interrupted", name);
            Thread.currentThread().interrupt();
        } finally {
            // 该方法结束时会立即释放 Leader，重新选举
            // 如果你想要要此实例一直是leader的话可以加一个死循环
            log.info("current Client: {} relinquishing leadership", name);
        }
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

}
