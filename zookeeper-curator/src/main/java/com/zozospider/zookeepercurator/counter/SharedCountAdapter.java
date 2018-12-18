package com.zozospider.zookeepercurator.counter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * SharedCount 适配器类
 * <p>
 * SharedCount 为其成员变量。同时实现了 SharedCount.addListener() 方法的参数 SharedCountListener 类，此 Listener 可以监听到计数器的事件。
 */
public class SharedCountAdapter implements SharedCountListener, Closeable {

    private final static Logger log = LoggerFactory.getLogger(SharedCountAdapter.class);

    // 客户端名称
    private final String name;
    // 计数器
    private SharedCount count;

    public SharedCountAdapter(CuratorFramework client, String path, String name) {
        this.name = name;
        // 创建 SharedCount 对象，并实现自我监听
        this.count = new SharedCount(client, path, 0);
        this.count.addListener(this);
    }

    public void start() throws Exception {
        count.start();
    }

    /**
     * 尝试更新（注意，此更新可能不成功）
     * <p>
     * 也可以在失败时多次调用该方法提高执行的成功率。
     *
     * @param newValue 更新后的最新指
     * @return 是否更新成功
     * @throws Exception
     */
    public boolean trySetCount(int newValue) throws Exception {

        log.info("newValue, current Client: {}, count trySetCount newValue: {} ...", name, newValue);
        /**
         * 尝试更新
         *
         * VersionedValue: 当前的 VersionedValue，如果期间其它 client 更新了此计数值，此次更新可能不成功。
         * newCount: 最新值
         */
        boolean bool = count.trySetCount(count.getVersionedValue(), newValue);
        if (bool) {
            log.info("newValue, current Client: {}, count trySetCount successfully", name);
        } else {
            log.warn("newValue, current Client: {}, count trySetCount unsuccessfully", name);
        }
        return bool;
    }

    /**
     * trySetCount() 的扩展方法，在原有基础上尝试新增（注意，此更新可能不成功）
     *
     * @param add 增量
     * @return 是否新增成功
     * @throws Exception
     */
    public boolean trySetCountAdd(int add) throws Exception {

        log.info("trySetCountAdd, current Client: {}, count trySetCount add: {} ...", name, add);
        boolean bool = count.trySetCount(count.getVersionedValue(), count.getCount() + add);
        if (bool) {
            log.info("trySetCountAdd, current Client: {}, count trySetCount add successfully", name);
        } else {
            log.warn("trySetCountAdd, current Client: {}, count trySetCount add unsuccessfully", name);
        }
        return bool;
    }

    /**
     * 强制更新
     *
     * @param newValue 更新后的最新指
     * @throws Exception
     */
    public void setCount(int newValue) throws Exception {

        count.setCount(newValue);
        log.info("setCount, current Client: {}, count setCount newValue: {} successfully", name, newValue);
    }

    /**
     * setCount() 的扩展方法，在原有基础上强制新增
     *
     * @param add 增量
     * @throws Exception
     */
    public void setCountAdd(int add) throws Exception {

        count.setCount(count.getCount() + add);
        log.info("setCountAdd, current Client: {}, count setCount add:{} successfully", name, add);
    }

    @Override
    public void countHasChanged(SharedCountReader sharedCount, int newCount) {
        log.info("current Client: {}, Counter's value is changed to {}", name, newCount);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        log.info("current Client: {}, State changed: {}", name, newState.toString());
    }

    @Override
    public void close() throws IOException {
        count.close();
    }
}
