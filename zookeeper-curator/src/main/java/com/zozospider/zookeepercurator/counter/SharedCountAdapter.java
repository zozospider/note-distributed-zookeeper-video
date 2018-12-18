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
     * 在原有基础上尝试新增
     *
     * @param add 增量
     * @throws Exception
     */
    public void trySetCount(int add) throws Exception {

        log.info("trySetCount, current Client: {}, count trySetCount add: {} ...", name, add);
        /**
         * 尝试更新
         *
         * VersionedValue: 当前的 VersionedValue，如果期间其它 client 更新了此计数值，此次更新可能不成功。
         * newCount: 最新值
         */
        boolean bool = count.trySetCount(count.getVersionedValue(), count.getCount() + add);
        if (bool) {
            log.info("trySetCount, current Client: {}, count trySetCount successfully", name);
        } else {
            log.warn("trySetCount, current Client: {}, count trySetCount unsuccessfully", name);
        }
    }

    /**
     * 在原有基础上强制新增
     *
     * @param add 增量
     * @throws Exception
     */
    public void setCount(int add) throws Exception {
        /**
         * 强制更新
         *
         * newCount: 最新值
         */
        count.setCount(count.getCount() + add);
        log.info("setCount, current Client: {}, count setCount successfully", name);
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
