package com.griddynamics.azinich.externalSort;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        String cur = System.getProperty("user.dir");
        String inputPath = cur + "/externalSort/input_test.txt";
        String outputPath = cur + "/externalSort/output.txt";
        Sorter sorter = new Sorter(inputPath, outputPath);
        sorter.run();
    }
}
