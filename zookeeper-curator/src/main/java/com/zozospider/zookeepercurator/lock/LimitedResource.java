package com.zozospider.zookeepercurator.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 模拟共享资源对象（在分布式环境下，source 资源只可被单线程使用）
 */
public class LimitedResource {

    private final static Logger log = LoggerFactory.getLogger(LimitedResource.class);

    // source 资源，只可被单线程使用
    private final AtomicBoolean source = new AtomicBoolean(false);

    /**
     * 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
     *
     * @param name 当前客户端名称
     * @throws InterruptedException
     */
    public void doSource(String name) throws InterruptedException {

        /**
         * situation one: 如果当前值为 false，则更新为 true，并返回 true。表示某个线程成功使用 source 资源。
         * situation two: 如果当前值是 true，则不更新，并返回 false。表示某个线程试图使用已经正在使用中的 source，则抛出异常提示资源被并发访问。
         * expect: 期望值
         * update: 新值
         */
        boolean bool = source.compareAndSet(false, true);
        if (bool) {
            // situation one
            log.info("doSource, current Client: {}, get source successfully", name);
        } else {
            // situation two
            log.error("doSource, current Client: {}, get source unsuccessfully, source should be used by one client at a time", name);
            throw new InterruptedException("doSource, get source unsuccessfully");
        }
        try {
            // 模拟资源正在被使用
            log.info("doSource, current Client: {}, using ...");
            Thread.sleep((long) (3 * Math.random()));
        } finally {
            // 使用完毕，将 source 重置为 false（可被使用状态）
            source.set(false);
        }

    }

}
