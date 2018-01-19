/**
 *     Copyright 2018 The Jarasandha.io project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.store.filesystem.cli;

import com.google.common.base.Stopwatch;
import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.api.StoreException;
import io.jarasandha.store.api.StoreException.StoreFullException;
import io.jarasandha.store.api.StoreWriter;
import io.jarasandha.store.filesystem.FileWriterParameters;
import io.jarasandha.store.filesystem.FileWriters;
import io.jarasandha.store.filesystem.NoOpFileWriteProgressListener;
import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.util.misc.Uuids;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * See calling class for docs.
 */
@Slf4j
class Importer implements Runnable {
    static final String OUTPUT_FILE_EXTENSION = "jara";
    static final String DIR_TEST_MODE = "/dev/null";
    private final List<File> inputFiles;
    private final File outputDir;
    private final FileWriterParameters writerParameters;

    Importer(List<File> inputFiles, File outputDir, FileWriterParameters writerParameters) {
        this.inputFiles = checkNotNull(inputFiles);
        this.outputDir = checkNotNull(outputDir);
        this.writerParameters = checkNotNull(writerParameters).validate();
    }

    @Override
    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        final WriterManager writerManager = new WriterManager(outputDir, writerParameters);
        log.info("Using parameters [{}]", writerParameters);
        try {
            if (inputFiles.isEmpty()) {
                inMemorySampleRecords(writerManager);
            } else {
                realFileRecords(inputFiles, writerManager);
            }
        } finally {
            try {
                Pair<Long, List<File>> output = writerManager.close();
                log.info("Processed total of [{}] records", output.getOne());
                log.info("Wrote to the following files:\n{}",
                        output.getTwo().stream().map(File::getAbsolutePath).collect(Collectors.joining("\n  "))
                );
            } catch (Exception e) {
                log.error("Error occurred while closing the output file", e);
            }
        }

        stopwatch.stop();
        double inSeconds = stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0;
        log.info("Completed in [{}] seconds", String.format("%.2f", inSeconds));
    }

    private static boolean isSpecialTestMode(File outputDir) {
        return DIR_TEST_MODE.equals(outputDir.getAbsoluteFile().getAbsolutePath());
    }

    private void realFileRecords(List<File> inputFiles, WriterManager writerManager) {
        final Stopwatch stopwatch = Stopwatch.createUnstarted();
        for (File inputFile : inputFiles) {
            try {
                log.debug("Reading file [{}]", inputFile.getAbsolutePath());
                stopwatch.reset().start();
                final int numRows;
                try (Stream<String> lineStream = Files.lines(inputFile.toPath(), UTF_8)) {
                    numRows = lineStream
                            .mapToInt(line -> {
                                final ByteBuf row = Unpooled.wrappedBuffer(line.getBytes(UTF_8));
                                try {
                                    writerManager.write(row);
                                } finally {
                                    row.release();
                                }
                                return 1;
                            })
                            .sum();

                    log.debug("Completed reading file with [{}] rows", numRows);
                }
                long millis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                log.info("Completed processing file with [{}] rows in [{}] millis", numRows, millis);
            } catch (IOException e) {
                log.error("Error occurred while processing file", e);
                return;
            }
        }
    }

    private void inMemorySampleRecords(WriterManager writerManager) {
        final String sampleRecordTemplate =
                "{\"name\":\"Melissa Franklin\",\"age\":5,\"friends\":[" +
                        "{\"name\":\"Lene Vestergaard Hau\",\"recentTrips\":[\"Tokyo\",\"Bangalore\"]}," +
                        "{\"name\":\"Edwin Powell Hubble\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Niels Bohr\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Latia Legler\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Edmond Halley\",\"recentTrips\":[\"Tokyo\",\"San Jose\"]}," +
                        "{\"name\":\"Maria Mitchell\",\"recentTrips\":[\"Bangalore\",\"Tokyo\"]}," +
                        "{\"name\":\"Virgil Viars\",\"recentTrips\":[\"Singapore\",\"Bangalore\"]}," +
                        "{\"name\":\"Caroline Herschel\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Chelsey Cranford\",\"recentTrips\":[\"San Jose\",\"San Jose\"]}," +
                        "{\"name\":\"Shirley Ann Jackson\",\"recentTrips\":[\"Tokyo\",\"San Jose\"]}," +
                        "{\"name\":\"Babette Buie\",\"recentTrips\":[\"Singapore\",\"Bangalore\"]}," +
                        "{\"name\":\"Patty Jo Watson\",\"recentTrips\":[\"Bangalore\",\"Bangalore\"]}," +
                        "{\"name\":\"Wolfgang Ernst Pauli\",\"recentTrips\":[\"San Jose\",\"Singapore\"]}," +
                        "{\"name\":\"Werner Karl Heisenberg\",\"recentTrips\":[\"Bangalore\",\"Bangalore\"]}," +
                        "{\"name\":\"Stephen Hawking\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Flossie Wong-Staal\",\"recentTrips\":[\"Tokyo\",\"Tokyo\"]}," +
                        "{\"name\":\"Albert Einstein\",\"recentTrips\":[\"Singapore\",\"Bangalore\"]}," +
                        "{\"name\":\"Ara Ashley\",\"recentTrips\":[\"Singapore\",\"San Jose\"]}," +
                        "{\"name\":\"Lord Kelvin\",\"recentTrips\":[\"Tokyo\",\"Singapore\"]}," +
                        "{\"name\":\"Max Born\",\"recentTrips\":[\"Bangalore\",\"San Jose\"]}," +
                        "{\"name\":\"Zella Zynda\",\"recentTrips\":[\"Singapore\",\"San Jose\"]}]," +
                        "\"checking\":{\"company\":\"Schwab\",\"balance\":708.32}," +
                        "\"saving\":{\"company\":\"Fidelity\",\"balance\":1781.25}," +
                        "\"retirement\":{\"company\":\"Schwab\",\"balance\":243.54}}";
        final int numSampleRecords = 2_000_000;
        final byte[] sampleRecord = sampleRecordTemplate.getBytes(UTF_8);

        log.info("Writing [{}] fixed records ", numSampleRecords, numSampleRecords);
        for (Integer i = 0; i < numSampleRecords; i++) {
            final ByteBuf row = Unpooled.wrappedBuffer(sampleRecord);
            try {
                writerManager.write(row);
            } finally {
                row.release();
            }
        }
    }

    private static class WriterManager {
        private final File outputDir;
        private final FileWriterParameters writerParameters;
        private final List<File> filesWrittenTo;
        private long totalRecordsCounter;
        private StoreWriter<FileId> fileWriter = null;

        private WriterManager(File outputDir, FileWriterParameters writerParameters) {
            this.outputDir = outputDir;
            this.writerParameters = writerParameters;
            this.filesWrittenTo = new LinkedList<>();
        }

        private LogicalRecordLocation write(ByteBuf record) {
            for (; ; ) {
                if (fileWriter == null) {
                    newFileWriter();
                }
                try {
                    return fileWriter.write(record);
                } catch (StoreFullException e) {
                    log.debug("Store [" + fileWriter.getStoreId() + "] full", e);
                    closeAndClearFileWriter();
                }
            }
        }

        private void newFileWriter() {
            FileId fileId = new FileId(Uuids.newUuid(), OUTPUT_FILE_EXTENSION);
            File file = isSpecialTestMode(outputDir) ? new File(DIR_TEST_MODE) : new File(outputDir, fileId.toString());
            Stopwatch stopwatch = Stopwatch.createUnstarted();

            NoOpFileWriteProgressListener progressListener = new NoOpFileWriteProgressListener() {
                private int numBlocksInStore;
                private int numRecordsInStore;
                private File currentFile;

                @Override
                public boolean storeStarted(FileInfo storeInfo) {
                    currentFile = storeInfo.file();
                    log.debug("Started writing to file [{}]", currentFile.getAbsolutePath());
                    stopwatch.reset().start();
                    numBlocksInStore = 0;
                    numRecordsInStore = 0;
                    return false;
                }

                @Override
                public void blockCompleted(FileBlockInfo blockInfo) {
                    int i = blockInfo.blockWithRecordSizes().numRecords();
                    totalRecordsCounter += i;
                    numBlocksInStore++;
                    numRecordsInStore += i;
                    log.debug("Block [{}] completed [{}] records", blockInfo.blockPosition(), i);
                }

                @Override
                public void storeClosed() {
                    double millis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                    long fileSizeBytes = currentFile.length();
                    double bytesPerSec = (fileSizeBytes / millis) * 1000.0;
                    String bps = format("%.3f", bytesPerSec);
                    log.info("Completed writing [{}] blocks ([{}] records) to file in [{}] millis @ [{}] B/sec",
                            numBlocksInStore, numRecordsInStore, millis, bps);
                    filesWrittenTo.add(currentFile);
                    currentFile = null;
                }

                @Override
                public void storeFailed(StoreException exception) {
                    currentFile = null;
                    if (!(exception instanceof StoreFullException)) {
                        log.error("Error occurred while writing to file", exception);
                    }
                }
            };

            fileWriter = FileWriters
                    .newFileWriter(fileId, file, progressListener, ByteBufAllocator.DEFAULT, writerParameters);
        }

        private void closeAndClearFileWriter() {
            if (fileWriter != null) {
                fileWriter.close();
                fileWriter = null;
            }
        }

        /**
         * @return The number of records written and all the files that were written to.
         */
        public Pair<Long, List<File>> close() {
            closeAndClearFileWriter();
            return Tuples.pair(totalRecordsCounter, filesWrittenTo);
        }
    }
}
