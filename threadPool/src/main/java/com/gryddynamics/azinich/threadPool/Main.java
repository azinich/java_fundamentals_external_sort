package com.gryddynamics.azinich.threadPool;

public class Main {
    public static void main(String[] args) {
        ThreadPool pool = ThreadPool.getThreadPull();
        pool.submit(() -> {
            try {
                System.out.println("Hi!");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 3000);

        System.out.println("BEFORE");

        try {
            Thread.sleep(4000);
        } catch (InterruptedException ignored) {

        }

        System.out.println("AFTER");

    }
}
