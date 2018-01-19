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

import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static io.jarasandha.store.filesystem.FileStatsEstimator.estimateUsingSizes;
import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class FileWriteProgressListenerTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testProgressListenerUnCompressed() throws IOException {
        final boolean compressBlocks = false;
        final boolean compressIndex = false;
        final boolean watchRecordCopies = false;
        internalTestProgress(compressBlocks, compressIndex, watchRecordCopies);
    }

    @Test
    public void testProgressListenerCompressed() throws IOException {
        final boolean compressBlocks = true;
        final boolean compressIndex = true;
        final boolean watchRecordCopies = true;
        internalTestProgress(compressBlocks, compressIndex, watchRecordCopies);
    }

    @Test
    public void testProgressListenerMixed1() throws IOException {
        final boolean compressBlocks = true;
        final boolean compressIndex = false;
        final boolean watchRecordCopies = false;
        internalTestProgress(compressBlocks, compressIndex, watchRecordCopies);
    }

    @Test
    public void testProgressListenerMixed2() throws IOException {
        final boolean compressBlocks = false;
        final boolean compressIndex = true;
        final boolean watchRecordCopies = true;
        internalTestProgress(compressBlocks, compressIndex, watchRecordCopies);
    }

    private void internalTestProgress
            (boolean compressBlocks, boolean compressIndex, boolean watchRecordCopies) throws IOException {

        final File file = folder.newFile();
        assertTrue(file.delete());

        final int sizeOfDataPartOfFileBytes = 5000;
        final int uncompressedBlockSizeLimitBytes = 1024;
        final int avgRecordSizeBytes = Uuids.UUID_BYTES_SIZE;
        FileStatsEstimator fileStatsEstimator =
                estimateUsingSizes(sizeOfDataPartOfFileBytes, uncompressedBlockSizeLimitBytes, avgRecordSizeBytes);

        final Queue<ByteBuf> copiesOfRecords = new LinkedList<>();
        final DebugFileWriteProgressListener
                progressListener = new DebugFileWriteProgressListener() {
            @Override
            public boolean storeStarted(FileInfo fileInfo) {
                super.storeStarted(fileInfo);
                return watchRecordCopies;
            }

            @Override
            protected void handleNonNullRecordCopy(ByteBuf recordCopy) {
                copiesOfRecords.add(recordCopy);
            }
        };
        final DebugFileWriter
                writer = new DebugFileWriter(
                file, fileStatsEstimator.estimatedSizeOfCompleteFileBytes(), uncompressedBlockSizeLimitBytes,
                allocator, progressListener,
                compressBlocks, compressIndex
        );

        final int numRecordsPerFile = 246;
        final int numExpectedBlocks = 4;
        final int numFullBlocks = 3;
        final int numRecordsInFullBlocks = 63;
        final int numRecordsInLastBlock = 57;

        //Same file.
        assertEquals(file, progressListener.fileInfo().file());
        assertEquals(compressBlocks, progressListener.fileInfo().compressedBlocks());
        //Nothing written yet.
        assertEquals(0, progressListener.recordsInUnflushedBlock());
        assertEquals(0, progressListener.completedFileBlockInfos().size());
        assertFalse(progressListener.closedOrFailed());

        final Queue<DebugLogicalRecordLocation> recordLocations = new LinkedList<>();
        for (int i = 0; i < numRecordsPerFile; i++) {
            ByteBuf record = Uuids.newUuidByteBuf();
            DebugLogicalRecordLocation location = writer.write(record);
            recordLocations.add(location);
            record.release();
        }

        //There should be 1 pending block.
        assertEquals(numRecordsInLastBlock, progressListener.recordsInUnflushedBlock());
        assertEquals(numFullBlocks, progressListener.completedFileBlockInfos().size());
        assertFalse(progressListener.closedOrFailed());

        writer.close();

        assertTrue(progressListener.fileInfo().file().exists());
        assertFalse(progressListener.fileInfo().fileChannel().isOpen());

        assertTrue(progressListener.closedOrFailed());
        assertEquals(0, progressListener.recordsInUnflushedBlock());
        assertEquals(numExpectedBlocks, progressListener.completedFileBlockInfos().size());
        assertEquals(numRecordsPerFile, recordLocations.size());

        int blockPosition = 0;
        for (FileBlockInfo fileBlockInfo : progressListener.completedFileBlockInfos()) {
            assertEquals(blockPosition, fileBlockInfo.blockPosition());
            if (blockPosition < numFullBlocks) {
                assertEquals(numRecordsInFullBlocks, fileBlockInfo.blockWithRecordSizes().recordByteSizes().size());
            } else {
                assertEquals(numRecordsInLastBlock, fileBlockInfo.blockWithRecordSizes().recordByteSizes().size());
            }
            //Cross-check each block and each record with the LLRs.
            fileBlockInfo
                    .blockWithRecordSizes()
                    .recordByteSizes()
                    .forEach(recordByteSize -> {
                        assertEquals(recordByteSize, avgRecordSizeBytes);

                        DebugLogicalRecordLocation llr = recordLocations.remove();
                        assertEquals(recordByteSize, llr.getRecordSizeBytes());
                        assertEquals(fileBlockInfo.blockStartFilePosition(), llr.getBlockStartByteOffset());
                        assertEquals(fileBlockInfo.blockPosition(), llr.getPositionOfBlock());

                        if (watchRecordCopies) {
                            ByteBuf copyOfRecord = copiesOfRecords.remove();
                            assertEquals(avgRecordSizeBytes, copyOfRecord.readableBytes());
                            copyOfRecord.release();
                        } else {
                            assertEquals(0, copiesOfRecords.size());
                        }
                    });
            blockPosition++;
        }

        //Make sure we've checked all of them.
        assertEquals(0, recordLocations.size());
        assertEquals(0, copiesOfRecords.size());
    }
}
