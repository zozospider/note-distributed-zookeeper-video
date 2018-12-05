package com.zozospider.zookeepercurator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.*;

public class CuratorClient {

    public CuratorFramework client;

    private RetryPolicy getRetryPolicy() {
        /**
         * 策略：
         * baseSleepTimeMs: 初始 sleep 的时间
         * maxRetries: 最大重试次数
         * maxSleepMs: 每次重试的最大间隔时间
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * 策略：重试一次
         * sleepMsBetweenRetry: 重试间隔的时间
         */
        RetryPolicy retryPolicy2 = new RetryOneTime(3000);

        /**
         * 策略：重试多次
         * n: 重试次数
         * sleepMsBetweenRetries: 每次重试间隔的时间
         */
        RetryPolicy retryPolicy3 = new RetryNTimes(3, 5000);

        /**
         * 策略：永远重试
         */
        RetryPolicy retryPolicy4 = new RetryForever(5000);

        /**
         * 策略：重试事件超过 maxElapsedTimeMs 后，就不再重试
         * maxElapsedTimeMs: 最大重试时间
         * sleepMsBetweenRetries: 每次重试间隔
         */
        RetryPolicy retryPolicy5 = new RetryUntilElapsed(2000, 3000);

        return retryPolicy3;
    }

    public void connect(String connectString) {
        RetryPolicy retryPolicy = getRetryPolicy();

    }

}
