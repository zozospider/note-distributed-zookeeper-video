package com.zozospider.zookeepercurator.lock.customize;

import com.zozospider.zookeepercurator.lock.InterProcessMutexOperator;
import com.zozospider.zookeepercurator.lock.LimitedResource;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义分布式锁（未成功实现）
 */
public class CustomizeLockOperator {

    private final static Logger log = LoggerFactory.getLogger(InterProcessMutexOperator.class);

    // 锁
    private CustomizeLock lock;
    // 共享资源对象
    private final LimitedResource resource;
    // 客户端名称
    private final String name;

    public CustomizeLockOperator(LimitedResource resource, String name,
                                 CuratorFramework client, String lockPath) throws Exception {
        this.resource = resource;
        this.name = name;
        log.info("CustomizeLockOperator, Client: C{}", name);
        // 通过 client 和 lockPath 确定一个锁
        lock = new CustomizeLock(client, lockPath);
    }

    public void doLockOnce(int j) throws Exception {

        log.info("doLockOnce, current Client: {}#{}, lock acquire ...", name, j);
        // 获取锁（此处如果有其他线程使用锁，则需阻塞等待直到其释放才能获取）
        lock.acquire();

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
            log.info("doLockOnce, current Client: {}#{}, lock has been released", name, j);
        }
    }



}
