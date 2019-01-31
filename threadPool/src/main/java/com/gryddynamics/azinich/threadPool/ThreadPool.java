package com.gryddynamics.azinich.threadPool;

import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool {

    private static ThreadPool threadPoolInstance;
    private static int threadNumber = 4;
    private final LinkedBlockingQueue<Tuple> queue;
    private final Worker[] workers;

    static {
        threadPoolInstance = new ThreadPool();
    }


    static ThreadPool getThreadPull() {
        return threadPoolInstance;
    }

    public static void setThreadNumber(int threadNumber) {
        ThreadPool.threadNumber = threadNumber;
    }

    private ThreadPool() {
        queue = new LinkedBlockingQueue<>();
        workers = new Worker[threadNumber];

        for (int i = 0; i < threadNumber; i++) {
            workers[i] = new Worker();
            workers[i].start();
        }

    }

    void submit(Runnable runnable, long sleepTime) {
        synchronized (queue) {
            queue.add(new Tuple(runnable, sleepTime));
            queue.notify();
        }
    }

    private class Worker extends Thread {
        public void run() {
            Tuple tuple;

            while (true) {
                synchronized (queue) {
                    while (queue.isEmpty()) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            System.out.println("An error occurred while waiting: " + e.getMessage());
                        }
                    }
                    tuple = queue.poll();
                }

                try {
                    Thread.sleep(tuple.getValue());
                    tuple.getKey().run();
                } catch (RuntimeException | InterruptedException e) {
                    System.out.println("Thread pool is interrupted due to: " + e.getMessage());
                }
            }
        }
    }

}
