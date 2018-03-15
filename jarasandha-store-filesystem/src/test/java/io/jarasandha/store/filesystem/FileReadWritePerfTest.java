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
package io.jarasandha.store.filesystem;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.filesystem.JsonSampleCreator.Record;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.Formatters;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTest;
import io.jarasandha.util.test.TestType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.jarasandha.store.filesystem.FileStatsEstimator.estimateUsingCounts;
import static io.jarasandha.util.collection.ImmutableBitSet.newBitSet;
import static io.jarasandha.util.collection.ImmutableBitSet.newBitSetWithAllPositionsTrue;
import static io.jarasandha.util.misc.Formatters.format;
import static io.jarasandha.util.misc.ProgrammaticLogManager.LOG_PATTERN_SMALL;
import static io.jarasandha.util.misc.ProgrammaticLogManager.setLoggerLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
@RunWith(Parameterized.class)
public class FileReadWritePerfTest {
    private static final TestType TEST_TYPE = TestType.QUICK_SANITY;
    private static TemporaryFolder folder = newTempFolder();
    private static List<TestExecutionInfo> executionInfos;
    private final TestInfo currentTestInfo;

    public FileReadWritePerfTest(TestInfo currentTestInfo) {
        this.currentTestInfo = currentTestInfo;
    }

    private static TemporaryFolder newTempFolder() {
        try {
            return new TemporaryFolder() {
                {
                    before();
                }
            };
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @BeforeClass
    public static void setUp() {
        executionInfos = new LinkedList<>();
    }

    @Parameters(name = "{index}: fileRead({0})")
    public static Iterable<Object[]> getTestParameters() throws IOException {
        setLoggerLevel(
                LOG_PATTERN_SMALL, Level.INFO,
                new HashMap<>(ImmutableMap.of(
                        Logger.ROOT_LOGGER_NAME, Level.WARN,
                        FileReadWritePerfTest.class.getName(), Level.INFO
                ))
        );

        final PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        final ImmutableList.Builder<TestInfo> testInfoBuilder = ImmutableList
                .<TestInfo>builder()

                /*Warmup*/
                .add(createTestData(allocator, 128 /*Records per block*/, 128 /*Approx num blocks*/, true, true))
                .add(createTestData(allocator, 128 /*Records per block*/, 128 /*Approx num blocks*/, true, true))
                .add(createTestData(allocator, 128 /*Records per block*/, 128 /*Approx num blocks*/, true, true))
                .add(createTestData(allocator, 128 /*Records per block*/, 128 /*Approx num blocks*/, true, true))

                /*Warmup*/
                .add(createTestData(allocator, 32, 512, true, true))
                .add(createTestData(allocator, 32, 512, true, true))
                .add(createTestData(allocator, 32, 512, true, true))
                .add(createTestData(allocator, 32, 512, true, true));

        if (TEST_TYPE == TestType.MEDIUM_DETAILED || TEST_TYPE == TestType.FULL) {
            testInfoBuilder
                /*Warmup*/
                    .add(createTestData(allocator, 32, 5120, true, true))
                    .add(createTestData(allocator, 32, 5120, true, true))
                    .add(createTestData(allocator, 32, 5120, true, true))
                    .add(createTestData(allocator, 32, 5120, true, true))

                    /*Warmup*/
                    .add(createTestData(allocator, 128, 1280, true, true))
                    .add(createTestData(allocator, 128, 1280, true, true))
                    .add(createTestData(allocator, 128, 1280, true, true))
                    .add(createTestData(allocator, 128, 1280, true, true));
        }

        if (TEST_TYPE == TestType.FULL) {
            testInfoBuilder
                    /*Real*/
                    .add(createTestData(allocator, 32, 51200, true, true))
                    .add(createTestData(allocator, 128, 12800, true, true))
                    .add(createTestData(allocator, 32, 51200, true, true))
                    .add(createTestData(allocator, 128, 12800, true, true));
        }

        log.info("Completed creating all test data [{}]", TEST_TYPE);
        log.info("");

        return testInfoBuilder
                .build()
                .stream()
                .map(testInfo -> new Object[]{testInfo})
                .collect(Collectors.toList());
    }

    private static TestInfo createTestData(
            ByteBufAllocator allocator,
            int numRecordsPerBlock, int approximateNumBlocks,
            boolean blocksCompressed, boolean indexCompressed) throws IOException {

        log.info("Creating test data for numRecordsPerBlock [{}], approximateNumBlocks [{}]," +
                        " blocksCompressed [{}], indexCompressed [{}]",
                numRecordsPerBlock, approximateNumBlocks, blocksCompressed, indexCompressed);

        final FileId fileId = new FileId(Uuids.newUuid(), "dat");
        final File file = folder.newFile(fileId.toString());
        assertTrue(file.delete());

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final ObjectMapper mapper = new ObjectMapper();
        final MutableIntIntMap blockPosAndNumRecords = new IntIntHashMap();

        Record record = JsonSampleCreator.newRecord(random);
        byte[] jsonRecord = mapper.writeValueAsBytes(record);
        final FileStatsEstimator estimator = estimateUsingCounts(
                approximateNumBlocks, numRecordsPerBlock, jsonRecord.length);
        log.info("Sample record size [{}] bytes and estimates [{}] in [{}]",
                format(jsonRecord.length), estimator, file.getName());

        AbstractTest.triggerGc();
        final Stopwatch stopwatch = Stopwatch.createStarted();

        try (
                FileWriter writer = new FileWriter(fileId, file,
                        estimator.estimatedSizeOfCompleteFileBytes(), estimator.uncompressedBlockSizeLimitBytes(),
                        allocator, new NoOpFileWriteProgressListener(),
                        blocksCompressed, indexCompressed, WriterParameters.SIZE_CHUNK_BYTES)
        ) {
            for (int i = 0; i < estimator.estimatedTotalNumRecords(); i++) {
                record = JsonSampleCreator.newRecord(random);
                jsonRecord = mapper.writeValueAsBytes(record);
                final ByteBuf utf8Record = Unpooled.wrappedBuffer(jsonRecord);
                try {
                    LogicalRecordLocation llr = writer.write(utf8Record);
                    blockPosAndNumRecords.updateValue(llr.getPositionOfBlock(), 0, oldValue -> oldValue + 1);
                } finally {
                    utf8Record.release();
                }
            }
        }

        stopwatch.stop();

        final TestInfo testInfo = new TestInfo(file, fileId, blockPosAndNumRecords.size(), blockPosAndNumRecords.sum());
        log.info(
                "Created test data:" +
                        "\n -- blocks [{}]\n -- a total of [{}] records\n -- avg [{}] records per block\n -- to [{}]" +
                        "\n -- index compressed [{}]\n -- blocks compressed [{}]\n -- file size [{}] bytes" +
                        "\n -- time taken [{}]",
                testInfo.numBlocks, format(testInfo.numRecords), blockPosAndNumRecords.average(),
                testInfo.file.getName(), indexCompressed, blocksCompressed, format(testInfo.file.length()),
                stopwatch);
        return testInfo;
    }

    @AfterClass
    public static void afterClass() {
        StringBuilder stringBuilder = new StringBuilder(1024)
                .append("Test summaries:\n")
                .append("name;numBlocks;numRecords;fileSizeBytes;swUnifiedMillis;swFileAndIndexReadMillis;" +
                        "swBlockReadMillis;swRecordReadMillis\n");

        for (TestExecutionInfo executionInfo : executionInfos) {
            stringBuilder
                    .append(executionInfo.name)
                    .append(';')
                    .append(executionInfo.testInfo.numBlocks)
                    .append(';')
                    .append(executionInfo.testInfo.numRecords)
                    .append(';')
                    .append(executionInfo.testInfo.file.length())
                    .append(';')
                    .append(executionInfo.swUnified.elapsed(TimeUnit.MILLISECONDS))
                    .append(';')
                    .append(executionInfo.swFileAndIndexRead.elapsed(TimeUnit.MILLISECONDS))
                    .append(';')
                    .append(executionInfo.swBlockRead.elapsed(TimeUnit.MILLISECONDS))
                    .append(';')
                    .append(executionInfo.swRecordRead.elapsed(TimeUnit.MILLISECONDS))
                    .append('\n');
        }
        log.info(stringBuilder.toString());

        folder.delete();
    }

    @Test
    public void testFileRead() {
        log.info("Starting readIndex test of [{}]", currentTestInfo);

        AbstractTest.triggerGc();

        final Stopwatch swUnified = Stopwatch.createUnstarted();
        final Stopwatch swFileAndIndexRead = Stopwatch.createUnstarted();
        final Stopwatch swBlockRead = Stopwatch.createUnstarted();
        final Stopwatch swRecordRead = Stopwatch.createUnstarted();

        swUnified.start();
        swFileAndIndexRead.start();
        try (
                FileReader fileReader = new FileReader(
                        currentTestInfo.file, currentTestInfo.fileId, new PooledByteBufAllocator(true),
                        new DebugTailReader(), new DebugBlockReader()
                )
        ) {
            //Warm up and readIndex all the blocks.
            Index<? extends BlockWithRecordOffsets> index = fileReader.readIndex(newBitSetWithAllPositionsTrue());
            swFileAndIndexRead.stop();
            assertEquals(currentTestInfo.numBlocks, index.blocks().size());

            int numRecordsAcrossBlocks = 0;
            //Traverse the blocks in reverse order to stress the page cache a little.
            for (int blockNum = currentTestInfo.numBlocks - 1; blockNum >= 0; blockNum--) {
                swBlockRead.start();
                final Pair<BlockWithRecordOffsets, ByteBuf> pair =
                        fileReader.readBlock(new FileBlockPosition(currentTestInfo.fileId, blockNum));
                final ByteBuf blockBytes = pair.getTwo();
                swBlockRead.stop();
                try {
                    swRecordRead.start();
                    BlockWithRecordOffsets block = index.blocks().get(blockNum);
                    //Iterate over records also in reverse.
                    for (int recordNum = block.numRecords() - 1; recordNum >= 0; recordNum--) {
                        final ByteBuf recordBytes =
                                fileReader.readRecord(blockBytes, new LogicalRecordLocation(blockNum, recordNum));
                        try {
                            assertTrue(recordBytes.readableBytes() > 0);
                        } finally {
                            recordBytes.release();
                        }
                        numRecordsAcrossBlocks++;
                    }
                    swRecordRead.stop();
                } finally {
                    blockBytes.release();
                }
            }
            assertEquals(currentTestInfo.numRecords, numRecordsAcrossBlocks);
        }
        swUnified.stop();

        TestExecutionInfo executionInfo = new TestExecutionInfo(
                "testFileRead", swFileAndIndexRead, swBlockRead, swRecordRead, swUnified, currentTestInfo);
        executionInfos.add(executionInfo);
        log.info(
                "Completed [testFileRead] readIndex test:" +
                        "\n -- file header and index readIndex time [{}]\n -- [{}] blocks total readIndex time [{}]" +
                        "\n -- [{}] records total readIndex time [{}]\n -- time taken [{}]",
                swFileAndIndexRead, currentTestInfo.numBlocks, swBlockRead,
                currentTestInfo.numRecords, swRecordRead, swUnified
        );
    }

    @Test
    public void testFileReadForEachBlock() {
        log.info("Starting readIndex test of [{}]", currentTestInfo);

        AbstractTest.triggerGc();
        final ByteBufAllocator allocator = new PooledByteBufAllocator(true);

        final Stopwatch swUnified = Stopwatch.createUnstarted();
        final Stopwatch swFileAndIndexRead = Stopwatch.createUnstarted();
        final Stopwatch swBlockRead = Stopwatch.createUnstarted();
        final Stopwatch swRecordRead = Stopwatch.createUnstarted();

        swUnified.start();
        int numRecordsAcrossBlocks = 0;
        //Traverse the blocks in reverse order to stress the page cache a little.
        for (int blockNum = currentTestInfo.numBlocks - 1; blockNum >= 0; blockNum--) {
            swFileAndIndexRead.start();
            try (
                    FileReader fileReader = new FileReader(
                            currentTestInfo.file, currentTestInfo.fileId, allocator,
                            new DebugTailReader(), new DebugBlockReader()
                    )
            ) {
                Index<? extends BlockWithRecordOffsets> index = fileReader.readIndex(newBitSet(blockNum));
                swFileAndIndexRead.stop();
                assertEquals(currentTestInfo.numBlocks, index.blocks().size());

                swBlockRead.start();
                final Pair<BlockWithRecordOffsets, ByteBuf> pair =
                        fileReader.readBlock(new FileBlockPosition(currentTestInfo.fileId, blockNum));
                final ByteBuf blockBytes = pair.getTwo();
                swBlockRead.stop();
                try {
                    swRecordRead.start();
                    BlockWithRecordOffsets block = index.blocks().get(blockNum);
                    //Iterate over records also in reverse.
                    for (int recordNum = block.numRecords() - 1; recordNum >= 0; recordNum--) {
                        final ByteBuf recordBytes =
                                fileReader.readRecord(blockBytes, new LogicalRecordLocation(blockNum, recordNum));
                        try {
                            assertTrue(recordBytes.readableBytes() > 0);
                        } finally {
                            recordBytes.release();
                        }
                        numRecordsAcrossBlocks++;
                    }
                    swRecordRead.stop();
                } finally {
                    blockBytes.release();
                }
            }
        }
        assertEquals(currentTestInfo.numRecords, numRecordsAcrossBlocks);
        swUnified.stop();

        TestExecutionInfo executionInfo = new TestExecutionInfo(
                "testFileReadForEachBlock", swFileAndIndexRead, swBlockRead, swRecordRead, swUnified, currentTestInfo);
        executionInfos.add(executionInfo);
        log.info(
                "Completed [testFileReadForEachBlock] readIndex test:" +
                        "\n -- file header and index readIndex time [{}]\n -- [{}] blocks total readIndex time [{}]" +
                        "\n -- [{}] records total readIndex time [{}]\n -- time taken [{}]",
                swFileAndIndexRead, currentTestInfo.numBlocks, swBlockRead,
                currentTestInfo.numRecords, swRecordRead, swUnified
        );
    }

    private static class TestExecutionInfo {
        private final String name;
        private final Stopwatch swFileAndIndexRead;
        private final Stopwatch swBlockRead;
        private final Stopwatch swRecordRead;
        private final Stopwatch swUnified;
        private final TestInfo testInfo;

        TestExecutionInfo(String name, Stopwatch swFileAndIndexRead, Stopwatch swBlockRead, Stopwatch swRecordRead,
                          Stopwatch swUnified, TestInfo testInfo) {
            this.name = name;
            this.swFileAndIndexRead = swFileAndIndexRead;
            this.swBlockRead = swBlockRead;
            this.swRecordRead = swRecordRead;
            this.swUnified = swUnified;
            this.testInfo = testInfo;
        }
    }

    private static class TestInfo {
        private final File file;
        private final FileId fileId;
        private final int numBlocks;
        private final long numRecords;

        TestInfo(File file, FileId fileId, int numBlocks, long numRecords) {
            this.file = file;
            this.fileId = fileId;
            this.numBlocks = numBlocks;
            this.numRecords = numRecords;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper("")
                    .add("length", Formatters.format(file.length()))
                    .add("numBlocks", numBlocks)
                    .add("numRecords", numRecords)
                    .add("file", file.getName())
                    .toString();
        }
    }
}
