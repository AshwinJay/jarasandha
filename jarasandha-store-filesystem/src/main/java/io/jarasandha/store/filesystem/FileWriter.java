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
import io.jarasandha.store.api.StoreException.RecordTooBigException;
import io.jarasandha.store.api.StoreException.StoreFullException;
import io.jarasandha.store.api.StoreException.StoreNotOpenException;
import io.jarasandha.store.api.StoreException.StoreWriteException;
import io.jarasandha.store.filesystem.index.BlockBuilder;
import io.jarasandha.store.filesystem.index.Builders;
import io.jarasandha.store.filesystem.index.IndexCodec;
import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.store.filesystem.tail.TailCodec;
import io.jarasandha.util.concurrent.Gate;
import io.jarasandha.util.io.ByteBufs;
import io.jarasandha.util.io.MoreCloseables;
import io.jarasandha.util.io.ReleasableByteBufs;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.NotThreadSafe;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.jarasandha.store.filesystem.FileHeaderCodec.writeHeader;
import static io.jarasandha.store.filesystem.index.IndexCodec.encode;
import static io.jarasandha.util.io.ByteBufs.SIZE_BYTES_CHECKSUM;
import static java.nio.file.StandardOpenOption.*;

/**
 * See {@link #write(ByteBuf)}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
@NotThreadSafe
class FileWriter implements StoreWriter<FileId> {
    private static final boolean SKIP_FILE_CHECK = false;

    //Start params.
    private final FileId storeId;
    @Getter
    private final File file;
    @Getter
    private final long fileSizeBytesLimit;
    @Getter
    private final int uncompressedBytesPerBlockLimit;
    private final ByteBufAllocator allocator;
    private final StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener;
    private final boolean sendCopiesToListener;
    private final boolean blocksCompressed;
    private final boolean indexCompressed;
    private final int writeChunkSizeBytes;
    //End params.

    /**
     * @see State
     */
    //Start state.
    private final Gate gate;
    private final FileChannel fileChannel;
    private final FastList<BlockWithRecordSizes> blocks;

    private long cleanFileSizeForRepair;
    @Getter
    private int recordsWrittenInBlockSoFar;
    private ByteBuf uncompressedBytesInBlock;
    private BlockBuilder inProgressBlockBuilder;
    private int maxRecordsPerBlock;
    //End state.

    /**
     * @param file
     * @param fileSizeBytesLimit
     * @param uncompressedBytesPerBlockLimit
     * @param allocator
     * @throws IllegalArgumentException
     * @throws StoreException
     * @see #FileWriter(FileId, File, long, int, ByteBufAllocator, StoreWriteProgressListener) with compressed blocks
     * and index.
     */
    FileWriter(FileId fileId, File file,
               long fileSizeBytesLimit, int uncompressedBytesPerBlockLimit,
               ByteBufAllocator allocator,
               StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener) {
        this(
                fileId, file, fileSizeBytesLimit, uncompressedBytesPerBlockLimit,
                allocator, listener, true, true, WriterParameters.SIZE_CHUNK_BYTES
        );
    }

    /**
     * @param fileId                         This should be the same name as the "file" parameter.
     * @param file
     * @param fileSizeBytesLimit
     * @param uncompressedBytesPerBlockLimit
     * @param allocator
     * @param blocksCompressed
     * @param indexCompressed
     * @param writeChunkSizeBytes
     * @throws IllegalArgumentException
     * @throws StoreException
     */
    FileWriter(
            FileId fileId, File file,
            long fileSizeBytesLimit, int uncompressedBytesPerBlockLimit,
            ByteBufAllocator allocator, StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener,
            boolean blocksCompressed, boolean indexCompressed, int writeChunkSizeBytes
    ) {

        final int minSizeOfFileBytes = estimateSizeOfFileBytes(1, 1);
        final int minSizeOfBlockBytes = minSizeOfBlockBytes(1);
        checkState(minSizeOfFileBytes > 0);
        checkState(minSizeOfFileBytes > minSizeOfBlockBytes);

        checkArgument(
                uncompressedBytesPerBlockLimit >= minSizeOfBlockBytes,
                "uncompressedBytesPerBlockLimit should be greater than [%s]", minSizeOfBlockBytes);
        checkArgument(
                fileSizeBytesLimit >= minSizeOfFileBytes,
                "fileSizeBytesLimit should be at least [%s]", minSizeOfFileBytes);
        checkArgument(
                fileSizeBytesLimit >= uncompressedBytesPerBlockLimit,
                "fileSizeBytesLimit should be at least same as uncompressedBytesPerBlockLimit");
        if (!SKIP_FILE_CHECK) {
            checkArgument(!file.exists(),
                    "Unable to create new file as there is either a directory or file already at [%s]",
                    file.getAbsolutePath());
        }
        checkArgument(writeChunkSizeBytes > 0,
                "writeChunkSizeBytes should be greater than 0 (and ideally a power of 2)");

        this.storeId = fileId;
        this.file = file;
        this.fileSizeBytesLimit = fileSizeBytesLimit;
        this.uncompressedBytesPerBlockLimit = uncompressedBytesPerBlockLimit;
        this.allocator = allocator;
        this.listener = listener;
        this.blocksCompressed = blocksCompressed;
        this.indexCompressed = indexCompressed;
        //Power of 2.
        this.writeChunkSizeBytes = Integer.highestOneBit(writeChunkSizeBytes - 1) << 1;

        log.debug("Starting [{}]", file.getAbsolutePath());
        this.gate = new Gate();
        try {
            File parentDir = file.getAbsoluteFile().getParentFile();
            if (!parentDir.exists() && parentDir.mkdirs()) {
                log.debug("Created directory [{}]", parentDir.getAbsolutePath());
            }
            this.fileChannel = FileChannel.open(file.toPath(), WRITE, (SKIP_FILE_CHECK ? CREATE : CREATE_NEW));
            this.cleanFileSizeForRepair = writeHeader(this.fileChannel, allocator);
            this.sendCopiesToListener =
                    this.listener.storeStarted(new FileInfo(fileId, blocksCompressed, file, fileChannel));
        } catch (IOException e) {
            throw new StoreWriteException("", e);
        }
        this.blocks = new FastList<>(8);
        log.debug("Started [{}]", file.getAbsolutePath());
    }

    @Override
    public FileId getStoreId() {
        return storeId;
    }

    /**
     * @param numBlocks
     * @param numRecordsPerBlock
     * @return The uncompressed size estimate where the block size is from {@link #minSizeOfBlockBytes(int)}.
     */
    static int estimateSizeOfFileBytes(int numBlocks, int numRecordsPerBlock) {
        checkArgument((numBlocks > 0 && numRecordsPerBlock >= 0) || (numBlocks == 0 && numRecordsPerBlock == 0));

        return FileHeaderCodec.sizeOfHeaderBytes() +
                minSizeOfBlockBytes(numBlocks) +
                IndexCodec.estimateSizeOfIndexBytes(numBlocks, numRecordsPerBlock) +
                TailCodec.sizeOfTailBytes();
    }

    /**
     * @param numBlocks
     * @return The uncompressed size estimate.
     */
    static int minSizeOfBlockBytes(int numBlocks) {
        checkArgument(numBlocks >= 0);
        return (numBlocks * SIZE_BYTES_CHECKSUM);
    }

    private static void checkOpen(Gate gate, File file) {
        if (gate.isClosed()) {
            throw new StoreNotOpenException(file.getAbsolutePath());
        }
    }

    private boolean isFileTooLong(long newFileSize) {
        return (newFileSize >= fileSizeBytesLimit);
    }

    private void checkFileSize(long newFileSize) {
        if (isFileTooLong(newFileSize)) {
            throw new StoreFullException(
                    "File [" + file.getAbsolutePath() + "] has a limit of [" + fileSizeBytesLimit + "] bytes");
        }
    }

    @Override
    public boolean isBlocksCompressed() {
        return blocksCompressed;
    }

    @Override
    public boolean isIndexCompressed() {
        return indexCompressed;
    }

    @CostlyOperation
    int estimateSizeOfIndexBytes() {
        checkOpen(gate, file);
        return IndexCodec.estimateSizeOfIndexBytes(blocks);
    }

    /**
     * Generally not meant to be called explicitly.
     *
     * @return True if the flush was performed.
     * <p>
     * Does nothing and returns false if the {@link #getRecordsWrittenInBlockSoFar()} is 0.
     * @throws StoreWriteException
     */
    @CostlyOperation
    @VisibleForTesting
    boolean flushBlock() {
        //Nothing to do.
        if (recordsWrittenInBlockSoFar == 0) {
            return false;
        }

        try {
            //Ensure contracts are valid.
            final long oldSync = cleanFileSizeForRepair;
            final long oldRecordsWrittenInBlockSoFar = recordsWrittenInBlockSoFar;
            checkState(oldRecordsWrittenInBlockSoFar > 0);
            checkState(uncompressedBytesInBlock != null);
            final long oldUncompressedBytesInBlockSoFar = uncompressedBytesInBlock.readableBytes();
            checkState(oldUncompressedBytesInBlockSoFar > 0);
            checkState(inProgressBlockBuilder != null);
            checkState(inProgressBlockBuilder.getRecordByteSizes().size() == recordsWrittenInBlockSoFar);

            log.trace("Starting block flush with [{}] records and [{}] bytes written so far",
                    oldRecordsWrittenInBlockSoFar, oldUncompressedBytesInBlockSoFar);

            final int numBytesWritten;
            final long blockEndPositionMinusChecksum;
            //Write all buffered values.
            if (blocksCompressed) {
                final ByteBuf compressedBytesInBlock = ByteBufs.compress(allocator, uncompressedBytesInBlock);
                try {
                    uncompressedBytesInBlock.release();

                    blockEndPositionMinusChecksum = oldSync + compressedBytesInBlock.readableBytes();
                    final long checksum = ByteBufs.calculateChecksum(compressedBytesInBlock);
                    compressedBytesInBlock.writeLong(checksum);
                    //Check size again before writing. Compressed size can sometimes be larger than raw size!
                    checkFileSize(oldSync + compressedBytesInBlock.readableBytes());
                    numBytesWritten = ByteBufs.writeOptimized(fileChannel, compressedBytesInBlock, writeChunkSizeBytes);
                } finally {
                    compressedBytesInBlock.release();
                }
            } else {
                blockEndPositionMinusChecksum = oldSync + uncompressedBytesInBlock.readableBytes();
                final long checksum = ByteBufs.calculateChecksum(uncompressedBytesInBlock);
                uncompressedBytesInBlock.writeLong(checksum);
                //Check size again before writing.
                checkFileSize(oldSync + uncompressedBytesInBlock.readableBytes());
                numBytesWritten = ByteBufs.writeOptimized(fileChannel, uncompressedBytesInBlock, writeChunkSizeBytes);
                uncompressedBytesInBlock.release();
            }
            uncompressedBytesInBlock = null;

            //Update the index too.
            BlockWithRecordSizes block = inProgressBlockBuilder.build(blockEndPositionMinusChecksum);
            maxRecordsPerBlock = Math.max(block.numRecords(), maxRecordsPerBlock);
            blocks.add(block);
            inProgressBlockBuilder = null;

            //Move the checkpoint forward.
            cleanFileSizeForRepair = oldSync + numBytesWritten;
            recordsWrittenInBlockSoFar = 0;

            final FileBlockInfo blockInfo =
                    new FileBlockInfo(blocks.size() - 1, block, oldSync, blockEndPositionMinusChecksum);
            listener.blockCompleted(blockInfo);
            log.trace("Completed block [{}] flush, new block is [{}]", oldSync, cleanFileSizeForRepair);
            return true;
        } catch (IOException io) {
            throw new StoreWriteException("", io);
        }
    }

    /**
     * Writes the given record if there is room in the file as determined by - {@link #getFileSizeBytesLimit()}.
     * <p>
     * Records are written in blocks as determined by the allowed size of the uncompressed block - {@link
     * #getUncompressedBytesPerBlockLimit()}.
     * <p>
     * Records are not immediately flushed to disk. Instead the disk flush is done:
     * <p>
     * - On {@link #close()} (if required)
     * <p>
     * - Or if the current block does not have sufficient room to accommodate the current record.
     * <p>
     * If block compression is enabled ({@link #isBlocksCompressed()}), then the block is first compressed and then the
     * compressed block is written to the file.
     *
     * @param value
     * @return
     * @throws StoreFullException    When the store cannot accommodate any further writes. This means, the next step is
     *                               to call {@link #close()}.
     * @throws RecordTooBigException When the record does not fit within the limits specified.
     * @throws StoreException        All other cases and an indication that something went wrong.
     */
    @Override
    public LogicalRecordLocation write(@CallerMustRelease ByteBuf value) {
        checkOpen(gate, file);
        checkArgument(value != null, "value cannot be null");
        final int sizeBytesOfValue = value.readableBytes();
        checkArgument(sizeBytesOfValue > 0, "value has not readable bytes");

        final ByteBuf copyOfValue = sendCopiesToListener ? value.copy() : null;
        try {
            //Until room on file is available.
            boolean flushed = false;
            while (true) {
                if (uncompressedBytesInBlock == null) {
                    uncompressedBytesInBlock =
                            allocator.buffer(uncompressedBytesPerBlockLimit, uncompressedBytesPerBlockLimit);
                }

                final long sizeBytesInBlockBeforeFlush = uncompressedBytesInBlock.readableBytes();
                //This is the uncompressed size check. Compressed size may be smaller and fit.
                final long blockWithMetadataByteSize = sizeBytesInBlockBeforeFlush + SIZE_BYTES_CHECKSUM;
                final int indexSizeEstimateBytes = IndexCodec.estimateSizeOfIndexBytes(
                        blocks.size() + 1, Math.max(maxRecordsPerBlock, recordsWrittenInBlockSoFar));
                final long newFileLenBytes =
                        cleanFileSizeForRepair + blockWithMetadataByteSize + indexSizeEstimateBytes;
                if (newFileLenBytes >= fileSizeBytesLimit) {
                    flushBlock();
                    throw new StoreFullException("" +
                            "File [" + file.getAbsolutePath() + "] has a limit of [" + fileSizeBytesLimit +
                            "] uncompressed bytes and writing any more records may push the size beyond this limit");
                }

                //Will the new record fit in the block?
                final long newBlockSizeIfPut = blockWithMetadataByteSize + sizeBytesOfValue;
                if (newBlockSizeIfPut >= uncompressedBytesPerBlockLimit) {
                    //No records written in this new block and yet it cannot fit this new record.
                    if (sizeBytesInBlockBeforeFlush == 0) {
                        if (newBlockSizeIfPut == uncompressedBytesPerBlockLimit) {
                            //Perfect fit. No more room after this is written.
                            break;
                        } else {
                            throw new RecordTooBigException("" +
                                    "Record size of [" + sizeBytesOfValue + "] bytes is too big." +
                                    " Block can only fit [" + (uncompressedBytesPerBlockLimit - SIZE_BYTES_CHECKSUM) +
                                    "] uncompressed bytes"
                            );
                        }
                    } else {
                        //Flush the block and start afresh hoping that there will be room after the flush.
                        try {
                            flushBlock();
                            flushed = true;
                            //Loop back and check for limits after flush.
                        } catch (Throwable t) {
                            throw closeRepairThrow(new State(), file, listener, cleanFileSizeForRepair, t);
                        }
                    }
                } else {
                    break;
                }
            }

            //----------

            final long bytesInBlockAfterPossibleFlush = uncompressedBytesInBlock.readableBytes();
            final int recordsSoFarInBlock = recordsWrittenInBlockSoFar;
            final long currBlockStartByteOffset = cleanFileSizeForRepair;

            //New block.
            if (flushed || recordsWrittenInBlockSoFar == 0) {
                inProgressBlockBuilder = Builders.blockBuilder(currBlockStartByteOffset, blocksCompressed);
            }
            //Collect the value.
            uncompressedBytesInBlock.writeBytes(value);
            //Update state.
            recordsWrittenInBlockSoFar++;
            //Update the index.
            inProgressBlockBuilder.addRecordSizeBytes(sizeBytesOfValue);

            final LogicalRecordLocation llr = newLlr(
                    blocks.size(), recordsSoFarInBlock,
                    currBlockStartByteOffset, bytesInBlockAfterPossibleFlush, sizeBytesOfValue
            );
            listener.recordWritten(llr, copyOfValue);
            return llr;
        } catch (StoreFullException | RecordTooBigException e) {
            if (copyOfValue != null) {
                copyOfValue.release();
            }
            throw e;
        } catch (Throwable t) {
            if (copyOfValue != null) {
                copyOfValue.release();
            }
            throw closeRepairThrow(new State(), file, listener, cleanFileSizeForRepair, t);
        }
    }

    @VisibleForTesting
    LogicalRecordLocation newLlr(int positionOfBlock, int positionOfRecordInBlock,
                                 long byteOffsetOfBlockStart, long byteOffsetOfRecordInBlock, int sizeOfRecordBytes) {
        return new LogicalRecordLocation(positionOfBlock, positionOfRecordInBlock);
    }

    /**
     * @throws StoreException
     */
    @Override
    @CostlyOperation
    public void close() {
        checkOpen(gate, file);

        log.debug("Closing [{}]", file.getAbsolutePath());
        long fileSizeAtEnd = cleanFileSizeForRepair;
        try {
            //Final flush.
            flushBlock();
            fileSizeAtEnd = cleanFileSizeForRepair;

            //Write the index.
            final int numBlocks = blocks.size();
            final long fileSizeNow = cleanFileSizeForRepair;
            final ByteBuf encodedIndex = encode(allocator, blocks, indexCompressed);
            try {
                final int encodedIndexSize = encodedIndex.readableBytes();
                final int encodedTailSize = TailCodec.sizeOfTailBytes();

                final long newFileSize = fileSizeNow + encodedIndexSize + encodedTailSize;
                if (isFileTooLong(newFileSize)) {
                    log.warn("File length of [{}] on close will be [{}] and it will exceed the" +
                                    " specified limit of [{}] bytes",
                            file.getAbsolutePath(), newFileSize, fileSizeBytesLimit);
                }
                cleanFileSizeForRepair +=
                        ByteBufs.writeOptimized(fileChannel, encodedIndex, writeChunkSizeBytes);
                fileSizeAtEnd = cleanFileSizeForRepair;
            } finally {
                encodedIndex.release();
            }

            //Write the tail.
            Tail tail =
                    new Tail(blocksCompressed, indexCompressed, fileSizeNow, cleanFileSizeForRepair, numBlocks);
            cleanFileSizeForRepair += TailCodec.writeTail(fileChannel, allocator, tail);
            fileSizeAtEnd = cleanFileSizeForRepair;

            //Ensure everything was written.
            fileChannel.force(true);

            //Close all.
            listener.storeClosed();
            new State().close();
            if (log.isInfoEnabled()) {
                log.info("Closed [{}] with [{}] blocks and final size [{}]",
                        file.getAbsolutePath(), numBlocks, fileSizeAtEnd);
            }
        } catch (Throwable t) {
            throw closeRepairThrow(new State(), file, listener, fileSizeAtEnd, t);
        }
    }

    private static StoreException closeRepairThrow(
            State state, File file, StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener,
            long truncateFileTo, Throwable cause) {

        log.warn("Starting file [{}] repair after error", file.getName());

        final StoreException storeException = cause instanceof StoreException
                ? (StoreException) cause : new StoreWriteException("", cause);

        try {
            listener.storeFailed(storeException);

            try {
                state.close();
            } catch (Throwable t) {
                log.debug("Error occurred while closing resources", t);
                storeException.addSuppressed(t);
            }

            log.debug("Attempting to truncate file [{}] of length [{}] to [{}]",
                    file.getAbsolutePath(), file.length(), truncateFileTo);
            try (FileChannel fileChannel = FileChannel.open(file.toPath(), WRITE)) {
                fileChannel.truncate(truncateFileTo);
                fileChannel.force(true);
            } catch (IOException e) {
                log.debug("Error occurred while attempting to truncate file", e);
                storeException.addSuppressed(e);
            }

            if (truncateFileTo == 0) {
                log.debug("File [{}] is now empty and so it will be deleted", file.getAbsoluteFile());
                boolean deleted = file.delete();
                log.warn("File [{}] {} deleted", file.getAbsolutePath(), (deleted ? "was" : "could not be"));
            }
        } catch (Throwable t) {
            storeException.addSuppressed(t);
        }

        log.warn("Completed repair after error", cause);
        throw storeException;
    }

    private class State implements AutoCloseable {
        public void close() {
            MoreCloseables.close(gate, log);
            MoreCloseables.close(fileChannel, log);

            cleanFileSizeForRepair = 0;
            recordsWrittenInBlockSoFar = 0;
            if (uncompressedBytesInBlock != null) {
                MoreCloseables.close(new ReleasableByteBufs(uncompressedBytesInBlock), log);
                uncompressedBytesInBlock = null;
            }
            inProgressBlockBuilder = null;
        }
    }
}
