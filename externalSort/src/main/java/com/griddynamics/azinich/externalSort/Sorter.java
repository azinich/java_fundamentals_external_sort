package com.griddynamics.azinich.externalSort;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class Sorter {
    private final Path inputPath;
    private final Path outputPath;
    private int memoryToUseInMB = 10;
    static final int CHARS_IN_MB = 1_048_576;
    private List<Chunk> chunks;
    private int chunkSize;

    public Sorter(String inputPath, String outputPath) {
        this.inputPath = Paths.get(inputPath);
        this.outputPath = Paths.get(outputPath);
        chunkSize = CHARS_IN_MB * memoryToUseInMB;
    }

    public Sorter(String inputPath, String outputPath, int memoryToUseInMB) {
        this(inputPath, outputPath);
        this.memoryToUseInMB = memoryToUseInMB;
        chunkSize = CHARS_IN_MB * memoryToUseInMB;
    }

    public void run() throws IOException {
        checkInputPath();

        initChunks(chunkSize);
        sortChunks();
        System.out.println("Sorted");
        mergeChunks();
        clearTemp();
    }

    private void clearTemp() throws IOException {
        for (Chunk a : chunks) {
            a.clear();
        }
    }

    private void mergeChunks() throws IOException {
        chunks.forEach(c -> c.initQueue(chunks.size()));
        Map<Integer, String> map = new HashMap<>(chunks.size());
        Comparator<Map.Entry<Integer, String>> comparator = Comparator.comparing(Map.Entry::getValue);

        System.out.println("Merge. Init");

        for (int i = 0; i < chunks.size(); i++) {
            map.put(i, chunks.get(i).getFirst());
        }

        System.out.println("Merge. First elements est.");

        while (map.values().stream().anyMatch(Objects::nonNull)) {
            Map.Entry<Integer, String> min = Collections.min(map.entrySet(), comparator);
            String minValue = min.getValue();
            Files.write(outputPath, Collections.singletonList(minValue), APPEND, CREATE);

            Integer key = min.getKey();
            String newValue = chunks.get(key).getFirst();
            if (newValue != null)
                map.put(key, newValue);
            else
                map.remove(key);
        }
        System.out.println("MERGED");
    }


    private void initChunks(int chunkSize) throws IOException {
        long maxLinesCount = Files.lines(inputPath).count();
        System.out.println("Max Lines num: " + maxLinesCount);
        List<Chunk> chunks = new ArrayList<>();
        long pointer = 0;
        long linesCount = 0;

        do {
            String path = "__temp_sort" + chunks.size() + ".txt";
            Chunk chunk = new Chunk(pointer, chunkSize, inputPath, Paths.get(path));
            pointer += chunk.getChunkSize();
            chunks.add(chunk);
            linesCount += chunk.getLinesCount();
            System.out.println(String.format("chunk number: %s, ends on: %s", chunks.size(), pointer));
            System.out.println(String.format("lines count = %s, with max = %s", linesCount, maxLinesCount));
            System.out.println("-----------------");

        } while (linesCount < maxLinesCount);

        this.chunks = chunks;
    }

    private void sortChunks() throws IOException {
        for (Chunk c : chunks) {
            c.sortChunk();
        }
    }


    private void checkInputPath() throws FileNotFoundException {
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("No such input path: " + inputPath);
        }
    }
}
