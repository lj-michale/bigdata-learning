package com.luoj.task.learn.sink;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-27
 */
public class MultiThreadConsumerClient implements Runnable {

    private LinkedBlockingQueue<String> bufferQueue;

    public MultiThreadConsumerClient(LinkedBlockingQueue<String> bufferQueue, CyclicBarrier clientBarrier) {
        this.bufferQueue = bufferQueue;
    }

    @Override
    public void run() {
        String entity;
        while (true){
            // 从 bufferQueue 的队首消费数据
            entity = bufferQueue.poll();
            // 执行 client 消费数据的逻辑
            doSomething(entity);
        }
    }

    // client 消费数据的逻辑
    private void doSomething(String entity) {
        // client 积攒批次并调用第三方 api
    }
}

