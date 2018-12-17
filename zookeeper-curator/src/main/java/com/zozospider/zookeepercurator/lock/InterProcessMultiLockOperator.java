package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 多共享锁对象（Multi Shared Lock）
 * <p>
 * Multi Shared Lock 是一个锁的容器。当调用 acquire()，所有的锁都会被 acquire()，调用 release() 时所有的锁都被 release（失败被忽略）。
 * 即在它上面的请求释放操作都会传递给它包含的所有的锁。
 */
public class InterProcessMultiLockOperator {

    private final static Logger log = LoggerFactory.getLogger(InterProcessMultiLockOperator.class);

    // 锁容器
    private InterProcessMultiLock multiLock;
    // 锁
    private InterProcessLock lock1;
    private InterProcessLock lock2;
    // 共享资源对象
    private final LimitedResource resource;
    // 客户端名称
    private final String name;

    /**
     * 新建一个操作类
     *
     * @param resource  共享资源对象
     * @param name      客户端名称
     * @param client    ZooKeeper 客户端操作对象
     * @param lockPath1 需要加锁的路径1
     * @param lockPath2 需要加锁的路径2
     */
    public InterProcessMultiLockOperator(LimitedResource resource, String name,
                                         CuratorFramework client, String lockPath1, String lockPath2) {
        this.resource = resource;
        this.name = name;
        // 可重入共享锁（Shared Reentrant Lock）
        this.lock1 = new InterProcessMutex(client, lockPath1);
        // 不可重入共享锁（Shared Lock）
        this.lock2 = new InterProcessSemaphoreMutex(client, lockPath2);
        // 锁容器
        this.multiLock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));
    }

    private static final long TIME = 10l;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    /**
     * 获取一次锁容器并访问共享资源对象，完成后释放一次锁容器
     * <p>
     * （此处只获取一次锁就释放，所以可以成功，失败情况请见 doLockTwiceIncorrectly() 方法）
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doLockOnce(int j) throws Exception {

        log.info("doLockOnce, current Client: {}#{}, multiLock acquire ...", name, j);
        // 获取锁容器（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = multiLock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doLockOnce, current Client: {}#{}, multiLock acquire successfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
        } else {
            log.error("doLockOnce, current Client: {}#{}, multiLock acquire unsuccessfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
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
            // 释放锁容器
            multiLock.release();
            log.info("doLockOnce, current Client: {}#{}, multiLock has been released, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
        }
    }

    /**
     * 获取两次锁，再访问资源对象，然后释放两次锁
     * <p>
     * （因为锁容器中存在不可重入共享锁，所以该情况第一次获取的锁会成功，第二次获取的锁会一直阻塞，直到超时）
     * （另外，由于该方法被阻塞导致锁没有释放，所以后续所有线程获取锁都会失败）
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doLockTwiceIncorrectly(int j) throws Exception {

        log.info("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 1 ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        // 因为锁容器存在不可重入共享锁，所以会出现不可重入共享锁的情况
        // 一个线程有且只有第一个能获取到被释放的锁，如果获取到的锁未被释放，该线程的后续请求会一直阻塞在此方法处，直到超时
        boolean bool = multiLock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 1 successfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
        } else {
            log.error("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 1 unsuccessfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
            throw new InterruptedException("doLock, lock acquire 1 unsuccessfully");
        }

        log.info("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 2 ...", name, j);
        // 再次获取锁（因为该锁已被线程获取且未被释放，该请求会一直阻塞在此方法处，直到超时）
        boolean bool2 = multiLock.acquire(TIME, UNIT);
        if (bool2) {
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 2 successfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
        } else {
            log.error("doLockTwiceIncorrectly, current Client: {}#{}, multiLock acquire 2 unsuccessfully, lock1: {}, lock2: {}",
                    name, j, lock1.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
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
            multiLock.release();
            multiLock.release();
            log.info("doLockTwiceIncorrectly, current Client: {}#{}, multiLock has been released twice", name, j);
        }

    }

}
