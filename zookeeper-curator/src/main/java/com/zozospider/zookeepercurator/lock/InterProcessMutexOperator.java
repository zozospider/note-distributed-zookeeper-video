package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 可重入共享锁（Shared Reentrant Lock）
 * <p>
 * 同一个客户端（线程）在拥有一个 InterProcessMutex 锁时，一个线程在释放锁前可以多次调用 lock.acquire() 获取锁而不会阻塞。并在使用结束后，多次调用 lock.release() 释放锁。示例参考 doLockTwice() 方法。
 * <p>
 * acquire(): 获取锁
 * release(): 释放锁
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

    private static final long TIME = 10l;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    /**
     * 获取一次锁并访问共享资源对象，完成后释放一次锁
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doLockOnce(int j) throws Exception {

        log.info("doLockOnce, current Client: {}#{}, lock acquire ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = lock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doLockOnce, current Client: {}#{}, lock acquire successfully", name, j);
        } else {
            log.error("doLockOnce, current Client: {}#{}, lock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doLockOnce, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doLockOnce, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放锁
            lock.release();
        }
    }

    /**
     * 获取两次锁，再访问资源对象，然后释放两次锁
     * <p>
     * （因为使用可重入共享锁，所以两次都可以获取成功）
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doLockTwice(int j) throws Exception {

        log.info("doLockTwice, current Client: {}#{}, lock acquire 1 ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = lock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doLockTwice, current Client: {}#{}, lock acquire 1 successfully", name, j);
        } else {
            log.error("doLockTwice, current Client: {}#{}, lock acquire 1 unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire 1 unsuccessfully");
        }

        log.info("doLockTwice, current Client: {}#{}, lock acquire 2 ...", name, j);
        // 再次获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        // 因为是可重入共享锁，所以两次都可以获取成功
        boolean bool2 = lock.acquire(TIME, UNIT);
        if (bool2) {
            log.info("doLockTwice, current Client: {}#{}, lock acquire 2 successfully", name, j);
        } else {
            log.error("doLockTwice, current Client: {}#{}, lock acquire 2 unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire 2 unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doLockTwice, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doLockTwice, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放两次锁
            lock.release();
            lock.release();
        }

    }

}
