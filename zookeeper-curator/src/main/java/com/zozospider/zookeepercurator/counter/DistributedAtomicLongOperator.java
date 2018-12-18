package com.zozospider.zookeepercurator.counter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式 long 计数器（DistributedAtomicLong）操作类
 */
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

    /**
     * 获取当前值
     *
     * @throws Exception
     */
    public void get() throws Exception {
        AtomicValue<Long> value = count.get();
        log.info("get, current Client: {}, value succeeded: {}, value preValue: {}, value postValue: {}, value getStats: {}",
                name, value.succeeded(), value.preValue(), value.postValue(), value.getStats());
    }

    /**
     * 加一
     *
     * @throws Exception
     */
    public void increment() throws Exception {

        log.info("increment, current Client: {}, count increment ...", name);
        AtomicValue<Long> value = count.increment();
        if (value.succeeded()) {
            log.info("increment, current Client: {}, count increment successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.error("increment, current Client: {}, count increment unsuccessfully", name);
        }
    }

    /**
     * 减一
     *
     * @throws Exception
     */
    public void decrement() throws Exception {

        log.info("decrement, current Client: {}, count decrement ...", name);
        AtomicValue<Long> value = count.decrement();
        if (value.succeeded()) {
            log.info("decrement, current Client: {}, count decrement successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.error("decrement, current Client: {}, count decrement unsuccessfully", name);
        }
    }

    /**
     * 增加特定的值
     *
     * @param add 增量
     * @throws Exception
     */
    public void add(long add) throws Exception {

        log.info("add, current Client: {}, count add: {} ...", name, add);
        AtomicValue<Long> value = count.add(add);
        if (value.succeeded()) {
            log.info("add, current Client: {}, count add successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.error("add, current Client: {}, count add unsuccessfully", name);
        }
    }

    /**
     * 减去特定的值
     *
     * @param subtract 减量
     * @throws Exception
     */
    public void subtract(long subtract) throws Exception {

        log.info("subtract, current Client: {}, count subtract: {} ...", name, subtract);
        AtomicValue<Long> value = count.subtract(subtract);
        if (value.succeeded()) {
            log.info("subtract, current Client: {}, count subtract successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.error("subtract, current Client: {}, count subtract unsuccessfully", name);
        }
    }

    /**
     * 尝试更新（注意，此更新可能不成功）
     *
     * @param newValue 更新后的最新指
     * @throws Exception
     */
    public void trySet(long newValue) throws Exception {

        log.info("trySet, current Client: {}, count trySet: {} ...", name, newValue);
        AtomicValue<Long> value = count.trySet(newValue);
        if (value.succeeded()) {
            log.info("subtract, current Client: {}, count subtract successfully, from {} to {}", name, value.preValue(), value.postValue());
        } else {
            log.error("subtract, current Client: {}, count subtract unsuccessfully", name);
        }
    }

    /**
     * 强制更新
     *
     * @param newValue 更新后的最新指
     * @throws Exception
     */
    public void forceSet(long newValue) throws Exception {

        count.forceSet(newValue);
        log.info("forceSet, current Client: {}, count setCount newValue: {} successfully", name, newValue);
    }

}
