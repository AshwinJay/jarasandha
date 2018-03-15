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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Table;
import io.jarasandha.store.api.Block;
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.api.StoreException.StoreFullException;
import io.jarasandha.store.filesystem.JsonSampleCreator.Record;
import io.jarasandha.store.filesystem.index.Builders;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import static io.jarasandha.store.filesystem.FileStatsEstimator.estimateUsingSizes;
import static io.jarasandha.util.collection.ImmutableBitSet.newBitSetWithAllPositionsTrue;
import static io.jarasandha.util.misc.ByteSize.Unit.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class FileReadWriteTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private HashMap<Block, ByteBuf> decompressedBlocks;

    public FileReadWriteTest() {
        super(Level.TRACE);
    }

    @Before
    public void setUp() {
        decompressedBlocks = new HashMap<>();
    }

    @Test
    public void test_100Recs_1KBBlocks_CompressBoth() throws IOException {
        runTest(100, (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), true, true, true);
    }

    @Test
    public void test_100Recs_1KBBlocks_NoCompressBoth() throws IOException {
        runTest(100, (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), false, false, true);
    }

    @Test
    public void test_100Recs_1KBBlocks_CompressBlocksOnly() throws IOException {
        runTest(100, (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), true, false, true);
    }

    @Test
    public void test_100Recs_1KBBlocks_CompressIndexOnly() throws IOException {
        runTest(100, (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), false, true, true);
    }

    @Test
    public void test_10KRecs_10KBBlocks_CompressBoth() throws IOException {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        runTest(10_000, 10 * (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), true, true, false /*Consumes heap and slows test*/);
    }

    @Test
    public void test_10KRecs_100KBBlocks_CompressBoth() throws IOException {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        runTest(10_000, 100 * (int) KILOBYTES.toBytes(), 2L * GIGABYTES.toBytes(), true, true, false/*Consumes heap and slows test*/);
    }

    @Test
    public void test_1MRecs_1MBBlocks_CompressBoth() throws IOException {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        runTest(1_000_000, (int) MEGABYTES.toBytes(), 2L * GIGABYTES.toBytes(), true, true, false/*Consumes heap and slows test*/);
    }

    private void runTest(int numRecordsToWrite,
                         int uncompressedBytesPerBlockLimit,
                         long fileSizeBytesLimit,
                         boolean compressBlocks,
                         boolean compressIndex,
                         boolean verifyDataAndIndex) throws IOException {

        log.info("Starting test with" +
                        " numRecordsToWrite [{}], uncompressedBytesPerBlockLimit [{}]," +
                        " fileSizeBytesLimit [{}], compressBlocks [{}], compressIndex [{}]",
                numRecordsToWrite, uncompressedBytesPerBlockLimit, fileSizeBytesLimit, compressBlocks, compressIndex);

        final int recordSizeBytes = 2 * Uuids.UUID_BYTES_SIZE /*There are 2 UUIDs here*/;
        assertTrue(recordSizeBytes < uncompressedBytesPerBlockLimit);

        final FileId fileId = new FileId(Uuids.newUuid(), ".test");
        final File file = folder.newFile(fileId.toString());
        assertTrue(file.delete());
        final DebugFileWriter
                writer = new DebugFileWriter(
                file, fileSizeBytesLimit, uncompressedBytesPerBlockLimit,
                allocator, new NoOpFileWriteProgressListener()
                , compressBlocks, compressIndex
        );
        assertEquals(compressBlocks, writer.isBlocksCompressed());
        assertEquals(compressIndex, writer.isIndexCompressed());

        try {
            final HashMap<DebugLogicalRecordLocation, ByteBuf> records = new HashMap<>();
            final ListMultimap<Long, DebugLogicalRecordLocation> blockStartAndLocations =
                    LinkedListMultimap.create();
            final ByteBuf fixedPrefix = Uuids.newUuidByteBuf();
            for (int i = 0; i < numRecordsToWrite; i++) {
                if (i % (10_000) == 0) {
                    log.info("Writing record [{}]", i);
                }

                //Create a longer record with a shared prefix.
                ByteBuf record = Unpooled.copiedBuffer(fixedPrefix).writeBytes(Uuids.newUuidByteBuf());
                assertEquals(recordSizeBytes, record.readableBytes());
                //Write to the file.
                DebugLogicalRecordLocation location = writer.write(record);
                //Store an in-memory copy.
                if (verifyDataAndIndex) {
                    records.put(location, record);
                }
                //Store the location too.
                blockStartAndLocations.put(location.getBlockStartByteOffset(), location);
            }

            writer.close();
            log.info("Wrote [{}] bytes to file", file.length());
            assertTrue(file.length() > 0 && file.length() <= writer.getFileSizeBytesLimit());

            //Now retrieve the index from file and check that it matches the index created during close.
            final Index<? extends BlockWithRecordOffsets> indexFromFile;
            try (
                    FileReader fileReader = new FileReader(
                            file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()
                    )
            ) {
                indexFromFile = fileReader.readIndex(newBitSetWithAllPositionsTrue());
            }

            //Compare with the deserialized index.
            verifyLocationsWithIndex(recordSizeBytes, blockStartAndLocations, indexFromFile, verifyDataAndIndex);

            if (verifyDataAndIndex) {
                //Compare the actual records.
                verifyRecordsWithFile(records, file, fileId);
            }

            //Clear.
            records.values().forEach(ByteBuf::release);
        } finally {
            assertTrue(file.delete());
        }
    }

    private static void verifyLocationsWithIndex(
            final int recordSizeBytes,
            final ListMultimap<Long, DebugLogicalRecordLocation> blockStartAndLocations,
            final Index<? extends BlockWithRecordOffsets> index,
            final boolean detailedVerification) {

        final List<? extends BlockWithRecordOffsets> blocks = index.blocks();

        //Ensure actual written blocks are same number as blocks in the index.
        assertEquals(blockStartAndLocations.keySet().size(), blocks.size());
        //Ensure the record sizes as per the index are correct.
        for (BlockWithRecordOffsets block : blocks) {
            Builders.convertToRecordSizes(block, value -> assertEquals(recordSizeBytes, value));
        }

        if (detailedVerification) {
            int blockLogicalPos = 0;
            for (Entry<Long, Collection<DebugLogicalRecordLocation>> entry :
                    blockStartAndLocations.asMap().entrySet()) {
                BlockWithRecordOffsets block = blocks.get(blockLogicalPos);
                assertEquals(entry.getKey(), (Long) block.blockStartByteOffset());
                assertTrue(entry.getValue().size() > 0);

                //Test block contents.
                int recordLogicalPos = 0;
                for (DebugLogicalRecordLocation llr : entry.getValue()) {
                    assertEquals(llr.getBlockStartByteOffset(), block.blockStartByteOffset());
                    assertEquals(llr.getPositionOfRecordInBlock(), recordLogicalPos);
                    assertEquals(llr.getRecordStartByteOffsetInBlock(), recordLogicalPos * recordSizeBytes);
                    int byteOffsetFromIndex = block.recordByteStartOffsets().get(recordLogicalPos);
                    assertEquals(llr.getRecordStartByteOffsetInBlock(), byteOffsetFromIndex);
                    assertEquals(llr.getRecordSizeBytes(), Builders.convertToRecordSize(block, recordLogicalPos));
                    recordLogicalPos++;
                }
                blockLogicalPos++;
            }
        }
    }

    private void verifyRecordsWithFile(
            HashMap<DebugLogicalRecordLocation, ByteBuf> records, File file, FileId fileId) throws IOException {

        try (
                DebugFileReader fileReader = new DebugFileReader(
                        file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()
                ).validate()
        ) {
            for (Entry<DebugLogicalRecordLocation, ByteBuf> entry : records.entrySet()) {
                DebugLogicalRecordLocation expectedLocation = entry.getKey();
                //Reset because the initial file write consumed the bytes.
                ByteBuf expectedRecord = entry.getValue().resetReaderIndex();
                DebugFileRecordLocation actualLocation =
                        (DebugFileRecordLocation) fileReader.locate(
                                expectedLocation.getPositionOfBlock(), expectedLocation.getPositionOfRecordInBlock());

                BlockWithRecordOffsets block = actualLocation.getBlock();
                assertEquals(fileId, actualLocation.getFileId());
                assertEquals(file, actualLocation.getFile());
                assertEquals(expectedLocation.getBlockStartByteOffset(), block.blockStartByteOffset());
                assertEquals(
                        expectedLocation.getRecordStartByteOffsetInBlock(),
                        actualLocation.getRecordStartByteOffsetInBlock());
                assertEquals(expectedLocation.getRecordSizeBytes(), actualLocation.getRecordSizeBytes());

                ByteBuf decompressedBlock = decompressedBlocks.get(block);
                if (decompressedBlock == null) {
                    Pair<BlockWithRecordOffsets, ByteBuf> pair = fileReader.readBlock(block);
                    decompressedBlock = pair.getTwo();
                    decompressedBlocks.put(block, decompressedBlock);
                }

                final int m =
                        com.google.common.primitives.Ints.checkedCast(actualLocation.getRecordStartByteOffsetInBlock());
                final ByteBuf actualRecord = decompressedBlock.slice(m, actualLocation.getRecordSizeBytes());
                assertEquals(0, ByteBufUtil.compare(expectedRecord, actualRecord));
            }
        }
    }

    @Test
    public void testUncompressedDirectRecordReads() throws IOException {
        final FileId fileId = new FileId(Uuids.newUuid(), "dat");
        final File file = folder.newFile(fileId.toString());
        assertTrue(file.delete());
        final Map<LogicalRecordLocation, String> recordsWritten = new HashMap<>();

        //Write records.
        {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final ObjectMapper mapper = new ObjectMapper();

            Record record = JsonSampleCreator.newRecord(random);
            String jsonRecord = mapper.writeValueAsString(record);
            final int utf8RecordSize = jsonRecord.getBytes(UTF_8).length;
            final FileStatsEstimator estimator =
                    estimateUsingSizes(utf8RecordSize * 80, utf8RecordSize * 8, utf8RecordSize);

            log.info("Estimated [{}] records", estimator.estimatedTotalNumRecords());
            try (
                    FileWriter writer = new FileWriter(fileId, file,
                            estimator.estimatedSizeOfCompleteFileBytes(), estimator.uncompressedBlockSizeLimitBytes(),
                            allocator, new NoOpFileWriteProgressListener(),
                            false /*Blocks not compressed*/, true/*Index compressed*/,
                            WriterParameters.SIZE_CHUNK_BYTES)
            ) {
                for (int i = 0; i < estimator.estimatedTotalNumRecords(); i++) {
                    record = JsonSampleCreator.newRecord(random);
                    jsonRecord = mapper.writeValueAsString(record);
                    final ByteBuf utf8Record = ByteBufUtil.writeUtf8(allocator, jsonRecord);
                    try {
                        LogicalRecordLocation llr = writer.write(utf8Record);
                        recordsWritten.put(llr, jsonRecord);
                    } finally {
                        utf8Record.release();
                    }
                }
            }
            log.info("Wrote [{}] records", recordsWritten.size());
            assertTrue(recordsWritten.size() > 0);
        }

        try (
                FileReader fileReader = new FileReader(
                        file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()
                ).validate();
        ) {

            final List<? extends BlockWithRecordOffsets> blocks =
                    fileReader.readIndex(newBitSetWithAllPositionsTrue()).blocks();
            log.info("File has [{}] blocks", blocks.size());
            assertTrue(blocks.size() > 0);

            //Verify each record. Random order.
            for (Entry<LogicalRecordLocation, String> entry : recordsWritten.entrySet()) {
                final LogicalRecordLocation llr = entry.getKey();
                final BlockWithRecordOffsets block = blocks.get(llr.getPositionOfBlock());
                final ByteBuf bytesCollector = allocator.buffer();
                try {
                    long numBytesCollected = 0;
                    try (WritableByteChannel dst = Channels.newChannel(new ByteBufOutputStream(bytesCollector))) {
                        numBytesCollected =
                                fileReader.transferUncompressedRecord(block, llr.getPositionOfRecordInBlock(), dst);
                    }

                    assertEquals(bytesCollector.readableBytes(), numBytesCollected);
                    final String actualJsonRecord = bytesCollector.toString(UTF_8);
                    assertEquals(entry.getValue(), actualJsonRecord);
                } finally {
                    bytesCollector.release();
                }
            }
        }
    }

    @Test
    public void testUncompressedDirectBlockReads() throws IOException {
        final FileId fileId = new FileId(Uuids.newUuid(), "dat");
        final File file = folder.newFile(fileId.toString());
        assertTrue(file.delete());
        final Table</*Block pos*/Integer, /*Record pos*/Integer, String> recordsWritten = HashBasedTable.create();

        //Write records.
        {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final ObjectMapper mapper = new ObjectMapper();

            Record record = JsonSampleCreator.newRecord(random);
            String jsonRecord = mapper.writeValueAsString(record);
            final int utf8RecordSize = jsonRecord.getBytes(UTF_8).length;
            final FileStatsEstimator estimator =
                    estimateUsingSizes(utf8RecordSize * 80, utf8RecordSize * 8, utf8RecordSize);

            log.info("Estimated [{}] records", estimator.estimatedTotalNumRecords());
            try (
                    FileWriter writer = new FileWriter(fileId, file,
                            estimator.estimatedSizeOfCompleteFileBytes(), estimator.uncompressedBlockSizeLimitBytes(),
                            allocator, new NoOpFileWriteProgressListener(),
                            false /*Blocks not compressed*/, false /*Index not compressed*/,
                            WriterParameters.SIZE_CHUNK_BYTES)
            ) {
                for (int i = 0; i < estimator.estimatedTotalNumRecords(); i++) {
                    record = JsonSampleCreator.newRecord(random);
                    jsonRecord = mapper.writeValueAsString(record);
                    final ByteBuf utf8Record = ByteBufUtil.writeUtf8(allocator, jsonRecord);
                    try {
                        LogicalRecordLocation llr = null;
                        try {
                            llr = writer.write(utf8Record);
                        } catch (StoreFullException e) {
                            log.info("Occasionally, the write fails before it reaches the estimated num records", e);
                            break;
                        }
                        recordsWritten.put(llr.getPositionOfBlock(), llr.getPositionOfRecordInBlock(), jsonRecord);
                    } finally {
                        utf8Record.release();
                    }
                }
            }
            log.info("Wrote [{}] records", recordsWritten.size());
            assertTrue(recordsWritten.size() > 0);
        }

        try (
                FileReader fileReader = new FileReader(
                        file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()
                ).validate()
        ) {
            final List<? extends BlockWithRecordOffsets> blocks =
                    fileReader.readIndex(newBitSetWithAllPositionsTrue()).blocks();
            log.info("File has [{}] blocks", blocks.size());
            assertTrue(blocks.size() > 0);

            //Read entire block directly and then readIndex records from that in-memory copy.
            int blockPosition = 0;
            for (BlockWithRecordOffsets block : blocks) {
                final ByteBuf bytesCollector = allocator.buffer();
                try {
                    long numBytesCollected = 0;
                    try (WritableByteChannel dst = Channels.newChannel(new ByteBufOutputStream(bytesCollector))) {
                        numBytesCollected = fileReader.transferUncompressedBlock(block, dst);
                    }

                    long numBytesExpected = 0;
                    final int numRecordsInBlock = block.recordByteStartOffsets().size();
                    for (int recordPosInBlock = 0; recordPosInBlock < numRecordsInBlock; recordPosInBlock++) {
                        final String expectedStrRecord = recordsWritten.get(blockPosition, recordPosInBlock);
                        final ByteBuf expectedUtf8Record = ByteBufUtil.writeUtf8(allocator, expectedStrRecord);
                        try {
                            numBytesExpected += expectedUtf8Record.readableBytes();
                            int actualOffset = bytesCollector.readerIndex();
                            int expectedOffset = block.recordByteStartOffsets().get(recordPosInBlock);
                            assertEquals(expectedOffset, actualOffset);

                            final ByteBuf actualRecord = bytesCollector.readBytes(expectedUtf8Record.readableBytes());
                            try {
                                assertTrue(ByteBufUtil.equals(expectedUtf8Record, actualRecord));
                            } finally {
                                actualRecord.release();
                            }
                        } finally {
                            expectedUtf8Record.release();
                        }
                    }

                    assertEquals(numBytesExpected, numBytesCollected);
                    log.info("Block [{}] has [{}] bytes across [{}] records",
                            blockPosition, numBytesCollected, numRecordsInBlock);
                } finally {
                    bytesCollector.release();
                }
                blockPosition++;
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        decompressedBlocks.forEach((block, byteBuf) -> byteBuf.release());
    }
}
