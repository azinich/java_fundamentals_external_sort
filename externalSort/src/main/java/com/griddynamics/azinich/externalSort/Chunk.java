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
    private long chunkSize;
    private Path inputPath;
    private Path outputPath;
    private long linesCount;

    Chunk(long start, long chunkSize, Path inputPath, Path outputPath) {
        this.start = start;
        this.chunkSize = chunkSize;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    void sortChunk() throws IOException {
        List<String> chunk = getChunk();
        Collections.sort(chunk);
        Files.write(outputPath, chunk, APPEND, CREATE);
        inputPath = outputPath;
    }

    private List<String> getChunk() throws IOException {
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
        } finally {
            this.chunkSize = getCharsAmount(result);
            this.linesCount = result.size();
        }
        return result;
    }

    static long getCharsAmount(List<String> lines) {
        if (lines.isEmpty()) return -1;

        int chars = lines.stream().mapToInt(String::length).reduce(0, (a, b) -> a + b);
        int newLines = System.lineSeparator().length() * lines.size();

        return chars + newLines;
    }

    ChunkElementsQueue getQueue(int chunksNum) {
        return new ChunkElementsQueue(chunksNum);
    }

    long getChunkSize() {
        return chunkSize;
    }

    long getLinesCount() {
        return linesCount;
    }

    void clear() throws IOException {
        Files.delete(outputPath);
    }

    class ChunkElementsQueue {
        private final int chunksNum;
        private Queue<String> queue;
        private BufferedReader reader = null;
        private long lNum = 0;

        private ChunkElementsQueue(int chunksNum) {
            this.chunksNum = chunksNum;
            initQueue();
        }

        private void initQueue() {
            queue = new ArrayDeque<>();
        }

        String getNext() throws IOException {
            checkQueue();
            String chunkHead = queue.peek();
            if (chunkHead != null) queue.remove();
            return chunkHead;
        }

        private void fillQueue() throws IOException {
            checkReader();
            long window = (int) Math.ceil(((double) linesCount) / chunksNum);

            if (lNum < linesCount) {
                queue.addAll(getNextPack(window));
            }
        }

        private List<String> getNextPack(long window) throws IOException {
            List<String> lines = new ArrayList<>();
            for (long i = 0; i < window; i++) {
                String line = reader.readLine();
                if (line == null) {
                    lNum++;
                    break;
                }
                lines.add(line);
            }
            return lines;
        }

        private void checkReader() throws IOException {
            if (reader == null) reader = Files.newBufferedReader(inputPath);
        }

        private void checkQueue() throws IOException {
            if (queue == null) {
                initQueue();
            }
            if (queue.isEmpty()) {
                fillQueue();
            }
        }
    }
}
