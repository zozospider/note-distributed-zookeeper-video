package com.zozospider.zookeepercurator.barrier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式屏障（DistributedBarrier）操作类
 */
public class DistributedBarrierOperator {

    private final static Logger log = LoggerFactory.getLogger(DistributedBarrierOperator.class);

    // 客户端名称
    private final String name;
    private DistributedBarrier barrier;

    public DistributedBarrierOperator(CuratorFramework client, String path, String name) {
        this.name = name;
        // 创建 DistributedBarrier 对象
        barrier = new DistributedBarrier(client, path);
    }

    /**
     * 设置栅栏
     *
     * @throws Exception
     */
    public void setBarrier() throws Exception {
        log.info("setBarrier, current Client: {} begin ...", name);
        barrier.setBarrier();
        log.info("setBarrier, current Client: {} end", name);
    }

    /**
     * 等待（如果设置了栅栏，此方法执行后会一直阻塞，直到其他客户端移除栅栏）
     *
     * @throws Exception
     */
    public void waitOnBarrier() throws Exception {
        log.info("waitOnBarrier, current Client: {} begin ...", name);
        // 如果设置了栅栏，此方法执行后会一直阻塞，直到其他客户端移除栅栏。
        barrier.waitOnBarrier();
        log.info("waitOnBarrier, current Client: {} end", name);
    }

    /**
     * 移除栅栏
     *
     * @throws Exception
     */
    public void removeBarrier() throws Exception {
        log.info("removeBarrier, current Client: {} begin ...", name);
        barrier.removeBarrier();
        log.info("removeBarrier, current Client: {} end", name);
    }

}
