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
package io.jarasandha.store.filesystem.index;

import ch.qos.logback.classic.Level;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.jarasandha.store.api.BlockWithRecordSizes;
import io.jarasandha.store.api.Index;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.collection.Ints;
import io.jarasandha.util.collection.MutableIntsWrapper;
import io.jarasandha.util.collection.SparseFixedSizeList;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static io.jarasandha.store.filesystem.index.IndexCodec.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class IndexCodecTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public IndexCodecTest() {
        super(Level.INFO);
    }

    @Test
    public void testRoundTrip() throws IOException {
        testRoundTripInternalWithCompression(0, 0);
    }

    @Test
    public void testRoundTripWithSomeHeader() throws IOException {
        testRoundTripInternalWithCompression(94, 0);
    }

    @Test
    public void testRoundTripWithSomeFooter() throws IOException {
        testRoundTripInternalWithCompression(0, 35);
    }

    @Test
    public void testRoundTripWithHeaderAndFooter() throws IOException {
        testRoundTripInternalWithCompression(470, 350);
    }

    @Test
    public void testRoundTripSpecificBlocksWithHeaderAndFooter() throws IOException {
        testRoundTripInternalWithCompression(
                470, 350, IndexCodec.SPARSE_LIST_MIN_SIZE * 2, 100, ImmutableBitSet.newBitSet(1, 4, 12, 14, 20)
        );
    }

    @Test
    public void testRoundTripNoBlocksToDecode() throws IOException {
        Index<BlockWithRecordSizes> index = testRoundTripInternalWithCompression(
                470, 350, IndexCodec.SPARSE_LIST_MIN_SIZE * 2, 100, ImmutableBitSet.newBitSet()
        );
        for (BlockWithRecordSizes block : index.blocks()) {
            assertEquals(BlockFactory.OFFSET_UNKNOWN, block.blockStartByteOffset());
            assertEquals(BlockFactory.OFFSET_UNKNOWN, block.blockEndByteOffset());
            assertEquals(0, block.numRecords());
        }
    }

    private void testRoundTripInternalWithCompression(int numStartBytesToFill, int numEndBytesToFill)
            throws IOException {
        final int numBlocks = 7;
        final int numRecordsPerBlock = 1700;
        testRoundTripInternalWithCompression(
                numStartBytesToFill, numEndBytesToFill, numBlocks, numRecordsPerBlock,
                /*Decode all blocks*/
                ImmutableBitSet.newBitSetWithAllPositionsTrue()
        );
    }

    private Index<BlockWithRecordSizes> testRoundTripInternalWithCompression(
            int numStartBytesToFill, int numEndBytesToFill,
            int numBlocks, int numRecordsPerBlock, ImmutableBitSet blockNumbersToDecode)
            throws IOException {

        final List<BlockWithRecordSizes> blocks = new FastList<>();
        long blockEndPosition = 0;
        for (int i = 0; i < numBlocks; i++) {
            final BlockBuilder builder = Builders.blockBuilder(3408 + i, true);
            blockEndPosition = builder.getBlockStartByteOffset();
            for (int j = 0; j < numRecordsPerBlock; j++) {
                final int recordSizeBytes = 800 + j;
                blockEndPosition += recordSizeBytes;
                builder.addRecordSizeBytes(recordSizeBytes);
            }
            blocks.add(builder.build( /*Assumes compression had not effect*/ blockEndPosition));
        }
        //Records per block is same across all blocks.
        assertEquals(estimateSizeOfIndexBytes(blocks), estimateSizeOfIndexBytes(numBlocks, numRecordsPerBlock));

        //Now add the one empty block.
        blocks.add(Builders.blockBuilder(3408 + numBlocks, true).build(3408 + numBlocks));

        final File file = folder.newFile();
        final Path path = file.toPath();
        final long indexStartOffset;
        final long indexEndOffset;
        try (OutputStream os = Files.newOutputStream(path, StandardOpenOption.APPEND)) {
            final int fillerByte = 'j';
            for (int i = 0; i < numStartBytesToFill; i++) {
                os.write(fillerByte);
            }
            os.flush();
            log.debug("Filled [{}] bytes at start and size of file is [{}]",
                    numStartBytesToFill, path.toFile().length());

            //------------

            indexStartOffset = file.length();
            ByteBuf indexV1ByteBuf = encode(allocator, blocks, true);
            final int sizeOfIndex = indexV1ByteBuf.readableBytes();
            try (InputStream is = new ByteBufInputStream(indexV1ByteBuf)) {
                ByteStreams.copy(is, os);
            }
            os.flush();
            indexEndOffset = file.length();
            log.debug("Filled [{}] index bytes and size of file is [{}]", sizeOfIndex, path.toFile().length());

            //------------

            for (int i = 0; i < numEndBytesToFill; i++) {
                os.write(fillerByte);
            }
            os.flush();
            log.debug("Filled [{}] bytes at end and size of file is [{}]", numEndBytesToFill, path.toFile().length());
        }

        final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        final Index<BlockWithRecordSizes> decodedIndex = IndexCodec.decode(
                allocator, fileChannel, true, indexStartOffset, indexEndOffset,
                blocks.size(), Builders::buildBlockWithRecordSizes, blockNumbersToDecode);
        final Index<? extends BlockWithRecordSizes> expectedIndex = Builders.buildIndex(blocks);
        final int totalNumExpectedBlocks = expectedIndex.blocks().size();
        assertEquals(totalNumExpectedBlocks, decodedIndex.blocks().size());
        for (int blockNum = 0; blockNum < expectedIndex.blocks().size(); blockNum++) {
            BlockWithRecordSizes decodedBlock = decodedIndex.blocks().get(blockNum);
            if (blockNum == blockNumbersToDecode.nextSetBit(blockNum)) {
                assertEquals(expectedIndex.blocks().get(blockNum), decodedBlock);
            } else {
                assertTrue(decodedIndex.blocks() instanceof SparseFixedSizeList);
                //Skipped block.
                assertEquals(BlockFactory.OFFSET_UNKNOWN, decodedBlock.blockStartByteOffset());
                assertEquals(BlockFactory.OFFSET_UNKNOWN, decodedBlock.blockEndByteOffset());
                assertEquals(0, decodedBlock.recordByteSizes().size());
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Num blocks [{}]", totalNumExpectedBlocks);
            for (BlockWithRecordSizes block : expectedIndex.blocks()) {
                log.debug("Block [{}]", block);
                block.recordByteSizes().forEach(each -> log.debug("  {}", each));
            }
        }

        return decodedIndex;
    }

    @Test
    public void testSingleBlockNoCompression() throws IOException {
        final boolean compressBlock = false;
        final boolean compressIndex = false;
        //One index.
        final FastList<BlockWithRecordSizes> blocks = new FastList<>();
        //Just 1 block.
        final int blockStartPosition = 3344;
        BlockBuilder builder = Builders.blockBuilder(blockStartPosition, compressBlock);
        //Just 1 record in that block.
        final int recordByteSizeBytes = 512;
        builder.addRecordSizeBytes(recordByteSizeBytes);
        final SimpleBlockWithRecordSizes block = (SimpleBlockWithRecordSizes) builder
                .build(blockStartPosition + recordByteSizeBytes);
        block.validate(compressIndex);
        blocks.add(block);

        assertEquals(block.recordByteSizes().get(0),
                block.blockEndByteOffset() - block.blockStartByteOffset());

        ByteBuf encodedIndex = encode(allocator, blocks, compressIndex);
        try {
            //Works only if there is no compression.
            assertEquals(estimateSizeOfIndexBytes(1, 1), encodedIndex.readableBytes());

            File file = folder.newFile();
            assertTrue(file.delete());
            try (ByteBufInputStream bis = new ByteBufInputStream(encodedIndex)) {
                Files.copy(bis, file.toPath());
            }
            try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                Index indexNew = decode(
                        allocator, fileChannel, compressIndex, 0, file.length(),
                        blocks.size(), Builders::buildBlockWithRecordSizes,
                        ImmutableBitSet.newBitSetWithAllPositionsTrue()
                );
                assertEquals(Builders.buildIndex(blocks), indexNew);
                log.debug("New index is\n{}", indexNew);
            } catch (IOException e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        } finally {
            encodedIndex.release();
        }
    }

    @Test
    public void testRecordOffsets() {
        final int recordSize = 100;
        MutableIntList recordSizeBytes = new IntArrayList(recordSize, recordSize, recordSize, recordSize);
        SimpleBlockWithRecordSizes block = (SimpleBlockWithRecordSizes) Builders
                .buildBlockWithRecordSizes(
                        0, recordSize * recordSizeBytes.size(),
                        new MutableIntsWrapper(recordSizeBytes)
                );
        block.validate(false);
        SimpleBlockWithRecordOffsets block2 = (SimpleBlockWithRecordOffsets) Builders
                .buildBlockWithRecordOffsets(
                        block.blockStartByteOffset(),
                        block.blockEndByteOffset(),
                        new MutableIntsWrapper(recordSizeBytes)
                );
        block2.validate(false);
        Ints recordByteOffsets = block2.recordByteStartOffsets();

        assertEquals(recordSizeBytes.size(), recordByteOffsets.size());
        assertEquals(0, recordByteOffsets.get(0));
        assertEquals(recordSize, recordByteOffsets.get(1));
        assertEquals(recordSize * 2, recordByteOffsets.get(2));
        assertEquals(recordSize * 3, recordByteOffsets.get(3));
    }

    @Test
    public void testNoRecordOffsets() {
        MutableIntList recordSizeBytes = new IntArrayList();
        SimpleBlockWithRecordSizes block = (SimpleBlockWithRecordSizes) Builders
                .buildBlockWithRecordSizes(0, 0, new MutableIntsWrapper(recordSizeBytes));
        block.validate(false);
        SimpleBlockWithRecordOffsets block2 = (SimpleBlockWithRecordOffsets) Builders
                .buildBlockWithRecordOffsets(
                        block.blockStartByteOffset(), block.blockEndByteOffset(), new MutableIntsWrapper());
        block2.validate(false);
        Ints recordByteOffsets = block2.recordByteStartOffsets();

        assertEquals(recordSizeBytes.size(), recordByteOffsets.size());
        assertEquals(0, recordByteOffsets.size());
    }

    @Test
    public void testOneRecordOffsets() {
        MutableIntList recordSizeBytes = new IntArrayList();
        recordSizeBytes.add(340);
        SimpleBlockWithRecordSizes block = (SimpleBlockWithRecordSizes) Builders
                .buildBlockWithRecordSizes(0, 340, new MutableIntsWrapper(recordSizeBytes));
        SimpleBlockWithRecordOffsets block2 = (SimpleBlockWithRecordOffsets) Builders
                .buildBlockWithRecordOffsets(
                        block.blockStartByteOffset(),
                        block.blockEndByteOffset(),
                        new MutableIntsWrapper(recordSizeBytes));
        block2.validate(false);
        Ints recordByteOffsets = block2.recordByteStartOffsets();

        assertEquals(recordSizeBytes.size(), recordByteOffsets.size());
        assertEquals(0, recordByteOffsets.get(0));
    }
}
