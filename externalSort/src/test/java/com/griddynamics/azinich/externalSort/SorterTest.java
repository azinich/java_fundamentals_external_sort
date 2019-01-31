package com.griddynamics.azinich.externalSort;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.*;

public class SorterTest {

    @Test(expected = FileNotFoundException.class)
    public void inputFileNotExists() throws IOException {
        String wrongInputPath = "./im/sure/no/such/input/path.txt";
        String outputPath = "./output.txt";
        Sorter sorter = new Sorter(wrongInputPath, outputPath);
        sorter.run();
    }
}