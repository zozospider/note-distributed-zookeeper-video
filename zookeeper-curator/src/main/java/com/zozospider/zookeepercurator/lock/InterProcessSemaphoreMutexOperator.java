package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 不可重入共享锁（Shared Lock）
 * 与可重入共享锁（Shared Reentrant Lock）相比，它不能在同一个线程中重入。即同一个客户端（线程）在释放锁前不能多次调用 lock.acquire()。
 */
public class InterProcessSemaphoreMutexOperator {

    private final static Logger log = LoggerFactory.getLogger(InterProcessSemaphoreMutexOperator.class);

    // 锁
    private InterProcessSemaphoreMutex lock;
    // 共享资源对象
    private final LimitedResource resource;
    // 客户端名称
    private final String name;

    public InterProcessSemaphoreMutexOperator(LimitedResource resource, String name,
                                              CuratorFramework client, String lockPath) {
        this.resource = resource;
        this.name = name;
        this.lock = new InterProcessSemaphoreMutex(client, lockPath);
    }

    private static final long TIME = 10l;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    /**
     * 获取一次锁并访问共享资源对象，完成后释放一次锁
     * <p>
     * （因为使用不可重入共享锁，此处只获取一次锁就释放，所以可以成功，失败情况请见 doLockTwiceIncorrectly() 方法）
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
     * （因为使用不可重入共享锁，所以该情况第一次获取的锁会成功，第二次获取的锁会一直阻塞，直到超时）
     * （另外，由于该方法被阻塞导致锁没有释放，所以后续所有线程获取锁都会失败）
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doLockTwiceIncorrectly(int j) throws Exception {

        log.info("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 1 ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        // 一个线程有且只有第一个能获取到被释放的锁，如果获取到的锁未被释放，该线程的后续请求会一直阻塞在此方法处，直到超时
        boolean bool = lock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 1 successfully", name, j);
        } else {
            log.error("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 1 unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire 1 unsuccessfully");
        }

        log.info("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 2 ...", name, j);
        // 再次获取锁（因为该锁已被线程获取且未被释放，该请求会一直阻塞在此方法处，直到超时）
        boolean bool2 = lock.acquire(TIME, UNIT);
        if (bool2) {
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 2 successfully", name, j);
        } else {
            log.error("doLockTwiceIncorrectly, current Client: {}#{}, lock acquire 2 unsuccessfully", name, j);
            throw new InterruptedException("doLock, lock acquire 2 unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doLockTwiceIncorrectly, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放锁
            lock.release();
            lock.release();
        }

    }

}
