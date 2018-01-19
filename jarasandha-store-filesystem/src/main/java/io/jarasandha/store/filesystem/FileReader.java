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

import com.google.common.annotations.VisibleForTesting;
import io.jarasandha.store.api.*;
import io.jarasandha.store.api.StoreException.StoreCorruptException;
import io.jarasandha.store.api.StoreException.StoreNotOpenException;
import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.store.filesystem.index.BlockReader;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.store.filesystem.tail.TailReader;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.collection.Ints;
import io.jarasandha.util.io.ByteBufs;
import io.jarasandha.util.io.MoreCloseables;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.jarasandha.util.misc.Validateable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.jarasandha.util.io.ByteBufs.SIZE_BYTES_CHECKSUM;
import static io.jarasandha.util.io.ByteBufs.decompress;
import static java.lang.String.format;

/**
 * Provides methods to readIndex the file written by {@link FileWriter}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
@Slf4j
class FileReader implements StoreReader<FileId>, AutoCloseable, Validateable<FileReader> {
    private final File file;
    private final FileId storeId;
    private final ByteBufAllocator allocator;
    private final Tail tail;
    private final BlockReader blockReader;
    private final FileChannel fileChannel;

    /**
     * @param file
     * @param storeId
     * @param allocator
     * @throws StoreReadException
     */
    FileReader(
            File file, FileId storeId, ByteBufAllocator allocator,
            TailReader tailReader, BlockReader blockReader
    ) {
        this.file = checkNotNull(file);
        this.storeId = checkNotNull(storeId);
        this.allocator = checkNotNull(allocator);
        this.blockReader = checkNotNull(blockReader);

        final Tail tail;
        final FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            try {
                tail = tailReader.read(storeId, allocator, fileChannel);
            } catch (Throwable t) {
                MoreCloseables.close(fileChannel, log);
                throw t;
            }
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreReadException(format("Error occurred while opening file [%s]", file.getAbsolutePath()), e);
        }
        this.tail = checkNotNull(tail);
        this.fileChannel = checkNotNull(fileChannel);
    }

    @Override
    public FileReader validate() {
        checkArgument(file.getName().equals(storeId.toString()), "Actual file name and storeId do not match");
        return this;
    }

    public File getFile() {
        return file;
    }

    @Override
    public FileId getStoreId() {
        return storeId;
    }

    @Override
    public boolean isBlocksCompressed() {
        return tail.isBlocksCompressed();
    }

    @Override
    public boolean isIndexCompressed() {
        return tail.isIndexCompressed();
    }

    @Override
    public int getNumBlocks() {
        return tail.getNumBlocks();
    }

    /**
     * This method can be used to warm up any caches (disk caches, in-memory caches if present). It can be done perhaps
     * when the reader is opened or periodically if in-memory caches are in use.
     *
     * @return
     */
    @CostlyOperation
    public final Index<? extends BlockWithRecordOffsets> readIndex(ImmutableBitSet blocksToRead) {
        return blockReader.read(allocator, fileChannel, tail, storeId, blocksToRead);
    }

    private static void ensureBlockNotCompressed(boolean compressedBlocks) {
        checkArgument(!compressedBlocks, "Block is compressed");
    }

    /**
     * @see #transferUncompressedBlock(Block, WritableByteChannel)
     */
    final long transferUncompressedBlock(int positionOfBlock, WritableByteChannel destination) {
        ensureBlockNotCompressed(tail.isBlocksCompressed());
        Block block = blockReader.read(allocator, fileChannel, tail, new FileBlockPosition(storeId, positionOfBlock));
        return transferUncompressedBlock(block, destination);
    }

    /**
     * Reads the entire block and returns the uncompressed (if compressed) bytes.
     * <p>
     * Due of the direct nature in which the block's contents are transferred, the checksum is not calculated.
     *
     * @param block
     * @return The number of bytes that were transferred.
     * @throws IllegalArgumentException If {@link Tail#isBlocksCompressed()} is true.
     * @throws StoreReadException
     * @throws StoreNotOpenException
     * @throws StoreCorruptException
     */
    final long transferUncompressedBlock(Block block, WritableByteChannel destination) {
        ensureBlockNotCompressed(tail.isBlocksCompressed());

        long blockStartByteOffset = block.blockStartByteOffset();
        final long blockEndByteOffset = block.blockEndByteOffset();
        final long count = (blockEndByteOffset - blockStartByteOffset);
        try {
            return fileChannel.transferTo(blockStartByteOffset, count, destination);
        } catch (ClosedChannelException e) {
            throw new StoreNotOpenException(
                    format("File channel for file id [%s] appears to be closed." +
                            " Operation may have to be retried", storeId), e);
        } catch (IOException e) {
            throw new StoreReadException(
                    format("Error occurred while attempting to readIndex file id [%s] and block [%d - %d]",
                            storeId, blockStartByteOffset, blockEndByteOffset), e);
        }
    }

    /**
     * @see #readBlock(BlockWithRecordOffsets)
     */
    final Pair<BlockWithRecordOffsets, ByteBuf> readBlock(FileBlockPosition blockPosition) {
        BlockWithRecordOffsets block = blockReader.read(allocator, fileChannel, tail, blockPosition);
        return readBlock(block);
    }

    /**
     * Reads the entire block, verifies the checksum and then returns the uncompressed (if compressed) bytes.
     *
     * @param block
     * @return Uncompressed block.
     * @throws StoreReadException
     * @throws StoreNotOpenException
     * @throws StoreCorruptException
     */
    public final Pair<BlockWithRecordOffsets, ByteBuf> readBlock(BlockWithRecordOffsets block) {
        final long blockStartByteOffset = block.blockStartByteOffset();
        final long blockEndByteOffset = block.blockEndByteOffset();
        final ByteBuf byteBuf = readBlock(
                allocator, file, fileChannel, storeId,
                tail.isBlocksCompressed(), blockStartByteOffset, blockEndByteOffset);
        return Tuples.pair(block, byteBuf);
    }

    private static ByteBuf readBlock(
            ByteBufAllocator allocator,
            File file, FileChannel fileChannel, FileId fileId,
            boolean compressedBlocks, long blockStartByteOffset, long blockEndByteOffset
    ) {
        try {
            final int blockSize = (int) (blockEndByteOffset - blockStartByteOffset);
            final int blockAndChecksumSize = blockSize + SIZE_BYTES_CHECKSUM;

            final ByteBuf blockAndChecksumBuf = allocator.buffer(blockAndChecksumSize, blockAndChecksumSize);
            try {
                blockAndChecksumBuf.writeBytes(fileChannel, blockStartByteOffset, blockAndChecksumSize);

                final ByteBuf blockBuf = blockAndChecksumBuf.retainedSlice(0, blockSize);
                try {
                    final ByteBuf expectedChecksumBuf =
                            blockAndChecksumBuf.retainedSlice(blockSize, SIZE_BYTES_CHECKSUM);
                    try {
                        final long actualChecksum = ByteBufs.calculateChecksum(blockBuf);
                        final long expectedChecksum = expectedChecksumBuf.readLong();
                        if (actualChecksum != expectedChecksum) {
                            throw new StoreCorruptException(format(
                                    "The checksum calculated [%d] for block [%d - %d] of file [%s]" +
                                            " does not match the expected checksum of [%d]",
                                    actualChecksum, blockStartByteOffset, blockEndByteOffset,
                                    file.getAbsolutePath(), expectedChecksum));
                        }
                    } finally {
                        expectedChecksumBuf.release();
                    }

                    if (compressedBlocks) {
                        final ByteBuf uncompressedBuf = decompress(allocator, blockBuf);
                        blockBuf.release();
                        return uncompressedBuf;
                    } else {
                        return blockBuf;
                    }
                } catch (Throwable t) {
                    blockBuf.release();
                    throw t;
                }
            } finally {
                blockAndChecksumBuf.release();
            }
        } catch (ClosedChannelException e) {
            throw new StoreNotOpenException(
                    format("File channel for file id [%s] appears to be closed." +
                            " Operation may have to be retried", fileId), e);
        } catch (IOException e) {
            throw new StoreReadException(
                    format("Error occurred while attempting to readIndex file id [%s] and block [%d - %d]",
                            fileId, blockStartByteOffset, blockEndByteOffset), e);
        }
    }

    /**
     * @see #transferUncompressedRecord(int, int, WritableByteChannel)
     */
    final long transferUncompressedRecord(
            int positionOfBlock, int positionOfRecordInBlock, WritableByteChannel destination) {

        ensureBlockNotCompressed(tail.isBlocksCompressed());
        BlockWithRecordOffsets block =
                blockReader.read(allocator, fileChannel, tail, new FileBlockPosition(storeId, positionOfBlock));
        return transferUncompressedRecord(block, positionOfRecordInBlock, destination);
    }

    /**
     * Due of the direct nature in which the block's contents are transferred, the checksum is not calculated.
     *
     * @param block
     * @param positionOfRecordInBlock
     * @param destination
     * @return The number of bytes that were transferred.
     * @throws IllegalArgumentException If {@link Tail#isBlocksCompressed()} is true.
     * @throws StoreReadException
     * @throws StoreNotOpenException
     * @throws StoreCorruptException
     */
    final long transferUncompressedRecord(
            BlockWithRecordOffsets block, int positionOfRecordInBlock, WritableByteChannel destination) {

        ensureBlockNotCompressed(tail.isBlocksCompressed());
        final long blockStartByteOffset = block.blockStartByteOffset();
        final FileRecordLocation plr = locate(block, positionOfRecordInBlock);
        final long physicalStartOfRecord = blockStartByteOffset + plr.getRecordStartByteOffsetInBlock();
        final long count = plr.getRecordSizeBytes();
        try {
            return fileChannel.transferTo(physicalStartOfRecord, count, destination);
        } catch (ClosedChannelException e) {
            throw new StoreNotOpenException(
                    format("File channel for file id [%s] appears to be closed." +
                            " Operation may have to be retried", storeId), e);
        } catch (IOException e) {
            throw new StoreReadException(
                    format("Error occurred while attempting to readIndex file id [%s], block [%d - %d] and record [%d]",
                            storeId, blockStartByteOffset, block.blockEndByteOffset(), positionOfRecordInBlock), e);
        }
    }

    /**
     * @see #locate(BlockWithRecordOffsets, int)
     * <p>
     * Calls {@link #decorateFileRecordLocation(File, FileId, BlockWithRecordOffsets, FileRecordLocation)}
     * in case the return value needs to be decorated.
     */
    final FileRecordLocation locate(int positionOfBlock, int positionOfRecordInBlock) {
        final BlockWithRecordOffsets block =
                blockReader.read(allocator, fileChannel, tail, new FileBlockPosition(storeId, positionOfBlock));
        FileRecordLocation frl = locate(block, positionOfRecordInBlock);
        return decorateFileRecordLocation(file, storeId, block, frl);
    }

    @VisibleForTesting
    FileRecordLocation decorateFileRecordLocation(
            File file, FileId fileId, BlockWithRecordOffsets block, FileRecordLocation original) {
        return original;
    }

    /**
     * Locates the physical address of the record within the file.
     *
     * @param block
     * @param positionOfRecordInBlock 0 based index of the record within the block, whose position is provided above.
     * @return
     */
    static FileRecordLocation locate(BlockWithRecordOffsets block, int positionOfRecordInBlock) {
        final Ints recordByteOffsets = block.recordByteStartOffsets();
        final int recordStartOffsetInBlock = recordByteOffsets.get(positionOfRecordInBlock);
        final int recordSizeBytesLong =
                (positionOfRecordInBlock == block.numRecords() - 1)
                        ?  /*If last record*/ block.lastRecordByteSize()
                        : recordByteOffsets.get(positionOfRecordInBlock + 1) - recordStartOffsetInBlock;
        final int recordSizeBytes = com.google.common.primitives.Ints.checkedCast(recordSizeBytesLong);

        return new FileRecordLocation(recordStartOffsetInBlock, recordSizeBytes);
    }

    final ByteBuf readRecord(@CallerMustRelease ByteBuf blockByteBuf, LogicalRecordLocation logical) {
        final FileRecordLocation physical = locate(logical.getPositionOfBlock(), logical.getPositionOfRecordInBlock());
        return readRecord(blockByteBuf, physical);
    }

    static ByteBuf readRecord(@CallerMustRelease ByteBuf blockByteBuf, FileRecordLocation physical) {
        return blockByteBuf
                //Copy a slice from the cached block.
                .copy(physical.getRecordStartByteOffsetInBlock(), physical.getRecordSizeBytes());
    }

    @Override
    public void close() {
        MoreCloseables.close(fileChannel, log);
        log.debug("Closed [{}]", file.getAbsolutePath());
    }
}
