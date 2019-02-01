package com.gryddynamics.azinich.threadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool {

    private static ThreadPool threadPoolInstance;
    private static int threadNumber = 4;
    private final LinkedBlockingQueue<Tuple> queue;
    private List<Worker> workers;


    static ThreadPool getThreadPull() {
        if (threadPoolInstance == null)
            threadPoolInstance = new ThreadPool();
        return threadPoolInstance;
    }

    private ThreadPool() {
        queue = new LinkedBlockingQueue<>();
        workers = new ArrayList<>(threadNumber);

        createWorkers(threadNumber);
    }

    public void interruptAll() {
        workers.forEach(Thread::interrupt);
    }

    public void submit(Runnable runnable, long sleepTime) {
        synchronized (queue) {
            queue.add(new Tuple(runnable, sleepTime));
            queue.notify();
        }
    }

    public void setThreadNumber(int threadNumber) {
        int prevThreadNumber = ThreadPool.threadNumber;

        if (threadNumber > prevThreadNumber) {
            createWorkers(threadNumber - prevThreadNumber);
        }
        if (threadNumber < prevThreadNumber) {
            interruptAll();
            createWorkers(threadNumber);
        }

        ThreadPool.threadNumber = threadNumber;
    }

    private void createWorkers(int workersNumber) {
        for (int i = 0; i < workersNumber; i++) {
            Worker w = new Worker();
            workers.add(w);
            w.start();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        interruptAll();
    }

    private class Worker extends Thread {

        public void run() {

            Tuple taskSleepPair;

            while (!Thread.interrupted()) {
                synchronized (queue) {
                    while (queue.isEmpty()) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            System.out.println("An error occurred while waiting: " + e.getMessage());
                        }
                    }
                    taskSleepPair = queue.poll();
                }

                try {
                    Long sleepTime = taskSleepPair.getValue();
                    Runnable task = taskSleepPair.getKey();

                    if (sleepTime != 0) {
                        Thread.sleep(sleepTime);
                    }
                    task.run();

                } catch (RuntimeException | InterruptedException e) {
                    System.out.println("Thread pool is interrupted due to: " + e.getMessage());
                }
            }
        }
    }

}
