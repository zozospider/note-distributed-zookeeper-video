package com.zozospider.zookeepercurator.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderLatchAdapter implements LeaderLatchListener, Closeable {

    private final static Logger log = LoggerFactory.getLogger(LeaderLatchAdapter.class);

    // 当前客户端 LeaderSelector 名称标识
    private final String name;
    // 真正参与选举的 LeaderLatch 对象
    private final LeaderLatch leaderLatch;
    // 统计
    private final AtomicInteger leaderCount = new AtomicInteger();

    public String getName() {
        return name;
    }

    public LeaderLatch getLeaderLatch() {
        return leaderLatch;
    }

    public LeaderLatchAdapter(CuratorFramework client, String path, String name) {
        this.name = name;
        // 创建 LeaderLatch 对象，并实现自我监听
        leaderLatch = new LeaderLatch(client, path, name);
        leaderLatch.addListener(this);
    }

    public void start() throws Exception {
        leaderLatch.start();
    }

    /**
     * 被选举为 Leader 时触发，一旦成为 Leader，将一直保持 Leader 身份，除非自身原因关闭或异常。
     */
    @Override
    public void isLeader() {
        log.info("current Client: {} is now the leader, It has been leader {} time(s) before ...", name, leaderCount.getAndIncrement());
    }

    @Override
    public void notLeader() {
        log.info("current Client: {} lost leader", name);
    }

    /**
     * 当前的 LeaderLatch 选举对象是否为 Leader
     *
     * @return 是否 Leader
     */
    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    @Override
    public void close() throws IOException {
        leaderLatch.close();
    }

}