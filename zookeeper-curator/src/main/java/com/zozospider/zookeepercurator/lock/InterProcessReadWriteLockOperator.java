package com.zozospider.zookeepercurator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 可重入读写锁（Shared Reentrant Read Write Lock）
 * <p>
 * 一个读写锁管理一对相关的锁。一个负责读操作，另外一个负责写操作。读操作在写锁没被使用时可同时由多个进程使用，而写锁在使用时不允许读（阻塞）。
 * <p>
 * 一个拥有写锁的线程可重入读锁，但是读锁却不能进入写锁。这也意味着写锁可以降级成读锁，比如请求写锁 -> 请求读锁 -> 释放读锁 -> 释放写锁。从读锁升级成写锁是不行的。
 */
public class InterProcessReadWriteLockOperator {

    private final static Logger log = LoggerFactory.getLogger(InterProcessReadWriteLockOperator.class);

    // 锁
    private final InterProcessReadWriteLock lock;
    // 读锁
    private final InterProcessMutex readLock;
    // 写锁
    private final InterProcessMutex writeLock;
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
    public InterProcessReadWriteLockOperator(LimitedResource resource, String name,
                                             CuratorFramework client, String lockPath) {
        this.resource = resource;
        this.name = name;
        // 通过 client 和 lockPath 确定一个锁
        this.lock = new InterProcessReadWriteLock(client, lockPath);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    private static final long TIME = 10l;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    /**
     * 获取一次读锁并访问共享资源对象，完成后释放一次读锁
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doWriteLock(int j) throws Exception {

        // 得到写锁
        log.info("doWriteLock, current Client: {}#{}, writeLock acquire ...", name, j);
        // 获取写锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = writeLock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doWriteLock, current Client: {}#{}, writeLock acquire successfully", name, j);
        } else {
            log.error("doWriteLock, current Client: {}#{}, writeLock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, writeLock acquire unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doWriteLock, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doWriteLock, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放写锁
            writeLock.release();
            log.info("doWriteLock, current Client: {}#{}, writeLock has been released", name, j);
        }

    }

    /**
     * 获取一次读锁，一次写锁，并访问共享资源对象，完成后释放一次读锁，一次写锁
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doWriteReadLock(int j) throws Exception {

        // 注意只能先得到写锁再得到读锁，不能反过来
        log.info("doWriteReadLock, current Client: {}#{}, writeLock acquire ...", name, j);
        // 获取写锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = writeLock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doWriteReadLock, current Client: {}#{}, writeLock acquire successfully", name, j);
        } else {
            log.error("doWriteReadLock, current Client: {}#{}, writeLock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, writeLock acquire unsuccessfully");
        }

        log.info("doWriteReadLock, current Client: {}#{}, readLock acquire ...", name, j);
        // 获取读锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool2 = readLock.acquire(TIME, UNIT);
        if (bool2) {
            log.info("doWriteReadLock, current Client: {}#{}, readLock acquire successfully", name, j);
        } else {
            log.error("doWriteReadLock, current Client: {}#{}, readLock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, readLock acquire unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doWriteReadLock, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doWriteReadLock, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放读写锁
            writeLock.release();
            log.info("doWriteReadLock, current Client: {}#{}, writeLock has been released", name, j);
            readLock.release();
            log.info("doWriteReadLock, current Client: {}#{}, readLock has been released", name, j);
        }

    }

    /**
     * 获取一次读锁，两次写锁，并访问共享资源对象，完成后释放一次读锁，两次写锁
     * <p>
     * （因为使用可重入读写锁，所以两次读锁都可以获取成功）
     *
     * @param j 当前线程循环下标志
     * @throws Exception
     */
    public void doWriteReadLockTwice(int j) throws Exception {

        // 注意只能先得到写锁再得到读锁，不能反过来
        log.info("doWriteReadLockTwice, current Client: {}#{}, writeLock acquire ...", name, j);
        // 获取写锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool = writeLock.acquire(TIME, UNIT);
        if (bool) {
            log.info("doWriteReadLockTwice, current Client: {}#{}, writeLock acquire successfully", name, j);
        } else {
            log.error("doWriteReadLockTwice, current Client: {}#{}, writeLock acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, writeLock acquire unsuccessfully");
        }

        log.info("doWriteReadLockTwice, current Client: {}#{}, readLock 1 acquire ...", name, j);
        // 获取读锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        boolean bool2 = readLock.acquire(TIME, UNIT);
        if (bool2) {
            log.info("doWriteReadLockTwice, current Client: {}#{}, readLock 1 acquire successfully", name, j);
        } else {
            log.error("doWriteReadLockTwice, current Client: {}#{}, readLock 1 acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, readLock 1 acquire unsuccessfully");
        }

        log.info("doWriteReadLockTwice, current Client: {}#{}, readLock 2 acquire ...", name, j);
        // 再次获取读锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        // 因为是可重入读写锁，所以两次都可以获取成功
        boolean bool3 = readLock.acquire(TIME, UNIT);
        if (bool3) {
            log.info("doWriteReadLockTwice, current Client: {}#{}, readLock 2 acquire successfully", name, j);
        } else {
            log.error("doWriteReadLockTwice, current Client: {}#{}, readLock 2 acquire unsuccessfully", name, j);
            throw new InterruptedException("doLock, readLock 2 acquire unsuccessfully");
        }

        try {
            // 试图使用 source 资源，如果有其他线程正在使用，则会抛出异常。
            log.info("doWriteReadLockTwice, current Client: {}#{}, doing ...", name, j);
            resource.doSource(name + "#" + j);

        } catch (Exception e) {
            // 有其他线程正在使用，则在此处理 doSource 异常（仅供测试，理论上不会出现，因为 lock 已经加锁）
            log.error("doWriteReadLockTwice, current Client: " + name + "#" + j + ", doSource error: " + e.getMessage(), e);
        } finally {
            // 释放读写锁（注意读锁需要释放两次）
            writeLock.release();
            log.info("doWriteReadLockTwice, current Client: {}#{}, writeLock has been released", name, j);
            readLock.release();
            readLock.release();
            log.info("doWriteReadLockTwice, current Client: {}#{}, readLock has been released twice", name, j);
        }

    }

}
