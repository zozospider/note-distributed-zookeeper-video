package com.zozospider.zookeepercurator.barrier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedDoubleBarrierOperator {

    private final static Logger log = LoggerFactory.getLogger(DistributedDoubleBarrierOperator.class);

    // 客户端名称
    private final String name;
    private DistributedDoubleBarrier barrier;

    public DistributedDoubleBarrierOperator(CuratorFramework client, String path, int memberQty, String name) {
        this.name = name;
        // 创建 DistributedDoubleBarrier 对象
        barrier = new DistributedDoubleBarrier(client, path, memberQty);
    }

    public void enter() throws Exception {
        log.info("enter, current Client: {} begin ...", name);
        // 此方法执行后会一直阻塞，直到所有客户端都调用 enter() 方法到 memberQty 次。
        barrier.enter();
        log.info("enter, current Client: {} end", name);
    }

    public void leave() throws Exception {
        log.info("leave, current Client: {} begin ...", name);
        barrier.leave();
        log.info("leave, current Client: {} end", name);
    }

}
