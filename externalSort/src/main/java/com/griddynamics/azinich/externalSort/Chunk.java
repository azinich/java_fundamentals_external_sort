package com.griddynamics.azinich.externalSort;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

class Chunk {
    private final long start;
    private final long initChunkSize;
    private long chunkSize = -2;
    private Path inputPath;
    private long pointer;
    private Path outputPath;
    private Queue<String> queue;
    private int chunksNum;
    private long linesCount;

    Chunk(long start, long initChunkSize, Path inputPath, Path outputPath) {
        this.start = start;
        this.initChunkSize = initChunkSize;
        this.pointer = start;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    void initQueue(int chunksNum) {
        this.chunksNum = chunksNum;
        queue = new ArrayDeque<>();
    }

    String getFirst() throws IOException {
        checkQueue();
        String chunkHead = queue.peek();
        if (chunkHead != null) queue.remove();
        return chunkHead;
    }

    private void checkQueue() throws IOException {
        if (queue == null) {
            initQueue(chunksNum);
        }
        if (queue.isEmpty()) {
            fillQueue();
        }
    }

    private BufferedReader reader = null;
    private boolean isEnd = false;
    private long lNum = 0;

    private void fillQueue() throws IOException {
        if (reader == null) reader = Files.newBufferedReader(inputPath);

        long window = (int) Math.ceil(((double) linesCount) / chunksNum);
        if (!isEnd && lNum < linesCount) {
            List<String> lines = new ArrayList<>();
            for (long i = 0; i < window; i++) {
                String line = reader.readLine();
                if (line == null) break;
                lines.add(line);
                lNum++;
            }
            queue.addAll(lines);
        }
    }

    public static long getCharsAmount(List<String> lines) {
        if (lines.isEmpty()) return -1;
        int chars = lines.stream().mapToInt(String::length).reduce(0, (a, b) -> a + b);
        int newLines = System.lineSeparator().length() * lines.size();
        return chars + newLines;
    }

    long getChunkSize() throws IOException {
        if (chunkSize == -2) {
            List<String> chunk = getChunk(start, initChunkSize);
            linesCount = chunk.size();
            chunkSize = getCharsAmount(chunk);
        }
        return chunkSize;
    }

    private List<String> getChunk(long start, long chunkSize) throws IOException {
        List<String> result = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(inputPath.toUri()), "r")) {
            file.seek(start);
            long size = 0;

            while (size < chunkSize) {
                String line = file.readLine();
                if (line == null) {
                    return result;
                }
                result.add(line);
                size += line.length();
            }
        }
        return result;
    }

    public void sortChunk() throws IOException {
        System.out.println(String.format("Sorting chunk. Start: %s; End: %s", start, start + chunkSize));
        List<String> chunk = getChunk(start, chunkSize - linesCount * System.lineSeparator().length());
        Collections.sort(chunk);
        Files.write(outputPath, chunk, APPEND, CREATE);
        inputPath = outputPath;
    }

    public long getLinesCount() {
        return linesCount;
    }

    public void clear() throws IOException {
        Files.delete(outputPath);
    }
}
