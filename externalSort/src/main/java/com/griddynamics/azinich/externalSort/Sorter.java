package com.griddynamics.azinich.externalSort;

import com.griddynamics.azinich.externalSort.Chunk.ChunkElementsQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class Sorter {
    private final Path inputPath;
    private final Path outputPath;
    private final int memoryToUseInMB = 50;
    private final int CHARS_IN_MB = 1_048_576;
    private List<Chunk> chunks;
    private int chunkSize;
    private Comparator<Entry<Integer, String>> comparator;

    public Sorter(String inputPath, String outputPath) {
        this.inputPath = Paths.get(inputPath);
        this.outputPath = Paths.get(outputPath);
        chunkSize = CHARS_IN_MB * memoryToUseInMB;
        comparator = Comparator.comparing(Entry::getValue);
    }

    public void run() throws IOException {
        checkInputPath();
        checkOutputPath();

        sortChunks();
        mergeChunks();
        clearTemp();
    }

    private void sortChunks() throws IOException {
        long maxLinesCount = Files.lines(inputPath).count();
        List<Chunk> chunks = new ArrayList<>();
        long pointer = 0;
        long linesCount = 0;

        do {
            String tempFilePath = String.format("__temp_sort%d.temp", chunks.size());
            Chunk chunk = new Chunk(pointer, chunkSize, inputPath, Paths.get(tempFilePath));

            chunk.sortChunk();
            chunks.add(chunk);

            pointer += chunk.getChunkSize();
            linesCount += chunk.getLinesCount();

        } while (linesCount < maxLinesCount);

        this.chunks = chunks;
    }

    private void mergeChunks() throws IOException {

        List<ChunkElementsQueue> queues = initQueues();
        Map<Integer, String> map = initMap(queues);

        while (map.values().stream().anyMatch(Objects::nonNull)) {
            Entry<Integer, String> min = getMinValue(map);

            printValue(min.getValue());

            Integer key = min.getKey();
            String newValue = queues.get(key).getNext();

            if (newValue != null)
                map.put(key, newValue);
            else
                map.remove(key);
        }
    }

    private Entry<Integer, String> getMinValue(Map<Integer, String> map) {
        return Collections.min(map.entrySet(), comparator);
    }

    private void printValue(String minValue) throws IOException {
        Files.write(outputPath, Collections.singletonList(minValue), APPEND, CREATE);
    }

    private Map<Integer, String> initMap(List<ChunkElementsQueue> queues) throws IOException {
        Map<Integer, String> map = new HashMap<>(chunks.size());

        for (int i = 0; i < chunks.size(); i++) {
            map.put(i, queues.get(i).getNext());
        }
        return map;
    }

    private List<ChunkElementsQueue> initQueues() {
        List<ChunkElementsQueue> queues = new ArrayList<>();
        for (Chunk c : chunks) {
            queues.add(c.getQueue(chunks.size()));
        }
        return queues;
    }

    private void clearTemp() throws IOException {
        for (Chunk a : chunks) {
            a.clear();
        }
    }

    private void checkOutputPath() throws IOException {
        if (Files.exists(outputPath)) {
            Files.delete(outputPath);
        }
    }

    private void checkInputPath() throws FileNotFoundException {
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("No such input path: " + inputPath);
        }
    }
}
