package com.zozospider.zookeepercurator.counter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedAtomicLongOperator {

    private final static Logger log = LoggerFactory.getLogger(DistributedAtomicLongOperator.class);

    // 客户端名称
    private final String name;
    private DistributedAtomicLong count;

    public DistributedAtomicLongOperator(CuratorFramework client, String path, String name) {
        this.name = name;
        // 创建 DistributedAtomicLong 对象
        count = new DistributedAtomicLong(client, path, new RetryNTimes(3, 5000));
    }

    public void get() throws Exception {
        AtomicValue<Long> value = count.get();
        log.info("get, current Client: {}, value succeeded: {}, value preValue: {}, value postValue: {}, value getStats: {}",
                name, value.succeeded(), value.preValue(), value.postValue(), value.getStats());
    }

    public void increment() throws Exception {

        log.info("increment, current Client: {}, count increment ...", name);
        AtomicValue<Long> value = count.increment();
        if (value.succeeded()) {
            log.info("increment, current Client: {}, count increment successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.info("increment, current Client: {}, count increment unsuccessfully", name);
        }
    }

    public void decrement() throws Exception {

        log.info("decrement, current Client: {}, count decrement ...", name);
        AtomicValue<Long> value = count.decrement();
        if (value.succeeded()) {
            log.info("decrement, current Client: {}, count decrement successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.info("decrement, current Client: {}, count decrement unsuccessfully", name);
        }
    }



}
