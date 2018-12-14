package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 可重入共享锁（Shared Reentrant Lock）
 * 同一个客户端在拥有锁的同时，可以在一个线程中多次调用 lock.acquire() 获取锁。
 * <p>
 * acquire(): 获取锁
 * release(): 释放锁
 * attemptRevoke(): 撤销当前锁
 */
public class InterProcessMutexOperator {

    private final static Logger log = LoggerFactory.getLogger(InterProcessMutexOperator.class);

    // 锁
    private InterProcessMutex lock;
    // 共享资源对象
    private final LimitedResource resource;
    // 客户端名称
    private final String name;

    /**
     * 新建一个操作类
     *
     * @param resource 共享资源对象
     * @param name     客户端名称
     * @param client   ZooKeeper 客户端操作对象
     * @param lockPath 需要加锁的路径
     */
    public InterProcessMutexOperator(LimitedResource resource, String name,
                                     CuratorFramework client, String lockPath) {
        this.resource = resource;
        this.name = name;
        // 通过 client 和 lockPath 确定一个锁
        this.lock = new InterProcessMutex(client, lockPath);
    }

    private static final long time = 10l;
    private static final TimeUnit unit = TimeUnit.SECONDS;

    /**
     * 获取锁并访问共享资源对象，完成后释放锁
     *
     * @throws Exception
     */
    public void doLock(int j) throws Exception {

        log.info("doLock, current Client: {}#{}, lock acquire ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = lock.acquire(time, unit);
        if (bool) {
            log.info("doLock, current Client: {}#{}, lock acquire successfully", name, j);
        } else {
            log.error("doLock, current Client: {}#{}, lock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire unsuccessfully");
        }
        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doLock, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doLock, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放锁
            lock.release();
        }
    }

}
