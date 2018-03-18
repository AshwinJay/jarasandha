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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.jarasandha.store.api.*;
import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.store.filesystem.index.BlockReader;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.TailReader;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.concurrent.Gate;
import io.jarasandha.util.concurrent.ManagedReference;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.tuple.Pair;

import java.io.File;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.jarasandha.util.misc.Formatters.format;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility class that reads {@link ByteBuf decompressed blocks} from the source files and caches them.
 * <p>
 * Key methods are:
 * <p>
 * - Read {@link Index} ({@link #readIndex(FileId, ImmutableBitSet)})
 * <p>
 * - Read entire block ({@link #readBlock(FileId, int)})
 * <p>
 * - Read a specific record ({@link #readRecord(FileId, LogicalRecordLocation)})
 * <p>
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
@Slf4j
public final class Readers implements StoreReaders<FileId> {
    public static final String METRICS_FILE_TAILS = "/fileTails";
    public static final String METRICS_INDEX_BLOCKS = "/indexBlocks";
    public static final String METRICS_FILE_READERS = "/fileReaders";
    public static final String METRICS_DECOMPRESSED_BLOCKS = "/decompressedBlocks";
    private final Gate gate;
    private final CacheAwareTailReader fileTailReader;
    private final CacheAwareBlockReader blockReader;
    private final CacheAwareDecompressedBlocks decompressedBlocks;
    private final LoadingCache<FileId, ManagedReference<FileReader>> fileReaders;
    private final AtomicInteger openFileReadersInCache;
    private final FileEventListener fileEventListener;
    private final Collection<String> metricNames;
    private final MetricRegistry metricRegistry;

    public Readers(ReadersParameters parameters) {
        parameters.validate();

        this.gate = new Gate();

        this.openFileReadersInCache = new AtomicInteger();

        final String metricsFamily;
        {
            File dir = parameters.files().getDirectory();
            StringBuilder joiner = new StringBuilder();
            joiner.append("/");
            if (dir.getParentFile() != null) {
                joiner
                        .append(dir.getParentFile().getName())
                        .append("/");
            }
            joiner.append(dir.getName());
            metricsFamily = joiner.toString();
        }

        this.fileTailReader = new CacheAwareTailReader(parameters, metricsFamily + METRICS_FILE_TAILS);

        this.blockReader = new CacheAwareBlockReader(parameters, metricsFamily + METRICS_INDEX_BLOCKS);

        this.fileReaders = Caffeine.<FileId, FileReader>newBuilder()
                .expireAfterAccess(parameters.expireAfterAccess().toNanos(), NANOSECONDS)
                .maximumSize(parameters.maxOpenFiles())
                .recordStats(() -> new CacheMetrics(parameters.metricRegistry(), metricsFamily + METRICS_FILE_READERS))
                .removalListener((RemovalListener<FileId, ManagedReference<FileReader>>) (key, value, removalCause) -> {
                    if (value != null) {
                        value.release();
                    }
                })
                .build(fileId -> {
                    final Stopwatch stopwatch = Stopwatch.createStarted();
                    if (log.isDebugEnabled()) {
                        log.debug("Opening file reader [{}]", fileId);
                    }
                    final File file = parameters.files().toFile(fileId);
                    parameters.fileEventListener().onMiss(fileId, file);

                    final FileReader fileReader = newFileReader(
                            fileId, file, parameters.allocator(), fileTailReader, blockReader
                    );
                    if (log.isDebugEnabled()) {
                        log.debug("Loaded file [{}] in [{}] millis",
                                fileId, stopwatch.stop().elapsed(MILLISECONDS));
                    }
                    this.openFileReadersInCache.incrementAndGet();
                    return new ManagedReference<>(
                            fileReader,
                            fileReaderToVerify -> true,
                            //Deallocator.
                            fileReaderToDeallocate -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("Closing file reader [{}]", fileId);
                                }
                                this.openFileReadersInCache.decrementAndGet();
                                parameters.fileEventListener().onRemove(fileId, file);
                                fileReaderToDeallocate.close();
                            }
                    );
                });

        this.decompressedBlocks =
                new CacheAwareDecompressedBlocks(parameters, metricsFamily + METRICS_DECOMPRESSED_BLOCKS);

        ImmutableList.Builder<String> metricNamesBuilder = ImmutableList.builder();
        String metricName = registerGauge(parameters.metricRegistry(),
                metricsFamily + METRICS_DECOMPRESSED_BLOCKS + "/inMemory/gauge/bytes",
                this.decompressedBlocks.getDecompressedBytesInCache()
        );
        metricNamesBuilder.add(metricName);
        metricName = registerGauge(parameters.metricRegistry(),
                metricsFamily + METRICS_FILE_READERS + "/open/gauge/counts", openFileReadersInCache);
        metricNamesBuilder.add(metricName);
        this.metricNames = metricNamesBuilder.build();
        this.metricRegistry = parameters.metricRegistry();

        this.fileEventListener = parameters.fileEventListener();
        this.fileEventListener.start();

        log.debug("Started");
    }

    public static FileReader newFileReader(FileId fileId, File file, ByteBufAllocator allocator) {
        return newFileReader(fileId, file, allocator, new DebugTailReader(), new DebugBlockReader());
    }

    private static FileReader newFileReader(
            FileId fileId, File file, ByteBufAllocator allocator, TailReader fileTailReader, BlockReader blockReader
    ) {
        return new FileReader(file, fileId, allocator, fileTailReader, blockReader);
    }

    private static String registerGauge(MetricRegistry metricRegistry, String name, Number actual) {
        metricRegistry.register(name, (Gauge<Long>) actual::longValue);
        return name;
    }

    /**
     * @param fileId
     * @param cause
     * @return
     * @throws StoreException
     */
    private static StoreException throwStoreException(FileId fileId, Throwable cause) {
        if (cause instanceof StoreException) {
            throw (StoreException) cause;
        }
        throw new StoreReadException(
                format("Error occurred while attempting to retrieve file reader for file id [%s]", fileId), cause);
    }

    @Override
    public Index<? extends BlockWithRecordOffsets> readIndex(FileId fileId, ImmutableBitSet blocksToRead) {
        return performOnFileReader(fileId, fileReader -> fileReader.readIndex(blocksToRead));
    }

    /**
     * Transfer the contents of the entire uncompressed block. This is a faster, low overhead version of
     * {@link #readBlock(FileId, int)}.
     *
     * @param fileId
     * @param positionOfBlock
     * @param destination
     * @return The number of bytes that were transferred.
     * @throws StoreException
     * @throws IllegalStateException
     */
    public long transferUncompressedBlock(FileId fileId, int positionOfBlock, WritableByteChannel destination) {
        return performOnFileReader(
                fileId, fileReader -> fileReader.transferUncompressedBlock(positionOfBlock, destination)
        );
    }

    /**
     * Transfer the contents of the uncompressed block's record. This is a faster, low overhead version of
     * {@link #readRecord(FileId, LogicalRecordLocation)}.
     *
     * @param fileId
     * @param positionOfBlock
     * @param positionOfRecordInBlock
     * @param destination
     * @return The number of bytes that were transferred.
     * @throws StoreException
     * @throws IllegalStateException
     */
    public long transferUncompressedRecord(
            FileId fileId, int positionOfBlock, int positionOfRecordInBlock, WritableByteChannel destination
    ) {
        return performOnFileReader(
                fileId, fileReader ->
                        fileReader.transferUncompressedRecord(positionOfBlock, positionOfRecordInBlock, destination)
        );
    }

    private <T> T performOnFileReader(FileId fileId, Function<FileReader, T> action) {
        for (int attempt = 1; ; attempt++) {
            final ManagedReference<FileReader> fileReaderRef = fileReaders.get(fileId);
            try {
                fileReaderRef.retain();
            } catch (IllegalReferenceCountException e) {
                log.trace("Attempt [{}] to retrieve file reader for file [{}] failed with [{}]",
                        attempt, fileId, e.getMessage());
                //Oops, it has already been released. So, try again.
                continue;
            }
            try {
                final FileReader fileReader = fileReaderRef.actualRef();
                return action.apply(fileReader);
            } finally {
                fileReaderRef.release();
            }
        }
    }

    /**
     * Read the entire block.
     *
     * @param fileId
     * @param positionOfBlock
     * @return Uncompressed block.
     * @throws StoreException
     * @throws IllegalStateException
     */
    @Override
    public ByteBuf readBlock(FileId fileId, int positionOfBlock) {
        return readBlockInternal(fileId, positionOfBlock).getDecompressedBlockBytes();
    }

    /**
     * Read the entire block.
     *
     * @param fileId
     * @param positionOfBlock
     * @return Uncompressed block.
     * @throws StoreException
     * @throws IllegalStateException
     */
    private DecompressedBlock readBlockInternal(FileId fileId, int positionOfBlock) {
        checkState(gate.isOpen());

        final FileBlockPosition fileBlockPosition = new FileBlockPosition(fileId, positionOfBlock);
        final DecompressedBlock blockToReturn;

        final DecompressedBlock blockFromCache = decompressedBlocks.slice(fileBlockPosition);
        //So, there was nothing in the cache.
        if (blockFromCache == null) {
            final Pair<BlockWithRecordOffsets, ByteBuf> blockReadPair = performOnFileReader(
                    fileId, fileReader -> fileReader.readBlock(fileBlockPosition)
            );
            final BlockWithRecordOffsets block = blockReadPair.getOne();
            final ByteBuf slicedByteBuf = blockReadPair.getTwo();
            try {
                //Then store it in the cache.
                final ByteBuf cacheableByteBuf = slicedByteBuf.retainedSlice();
                try {
                    decompressedBlocks.save(fileBlockPosition, new DecompressedBlock(block, cacheableByteBuf));
                } catch (Throwable t) {
                    cacheableByteBuf.release();
                    throw t;
                }
            } catch (Throwable t) {
                slicedByteBuf.release();
                throw throwStoreException(fileId, t);
            }
            blockToReturn = new DecompressedBlock(block, slicedByteBuf);
        } else {
            blockToReturn = blockFromCache;
        }

        if (log.isDebugEnabled()) {
            log.debug("Loaded block [{}] of file [{}] with [{}] bytes",
                    fileBlockPosition.getBlockPosition(), fileBlockPosition.toFileId(),
                    format(blockToReturn.getDecompressedBlockBytes().readableBytes()));
        }
        return blockToReturn;
    }

    /**
     * Read a specific record.
     *
     * @param fileId
     * @param logical
     * @return
     * @throws StoreReadException
     * @throws IllegalStateException
     */
    @Override
    public ByteBuf readRecord(FileId fileId, LogicalRecordLocation logical) {
        checkState(gate.isOpen());

        final DecompressedBlock blockResult = readBlockInternal(fileId, logical.getPositionOfBlock());
        try {
            FileRecordLocation flr = FileReader.locate(blockResult.getBlock(), logical.getPositionOfRecordInBlock());
            return FileReader.readRecord(blockResult.getDecompressedBlockBytes(), flr);
        } finally {
            blockResult.getDecompressedBlockBytes().release();
        }
    }

    /**
     * Release any open resources related to the store.
     *
     * @param fileId
     * @throws IllegalStateException
     * @throws StoreException
     */
    @Override
    public void release(FileId fileId) {
        checkState(gate.isOpen());

        fileReaders.invalidate(fileId);
        fileTailReader.invalidate(fileId);
    }

    /**
     * {@link #cleanUp() Purges} all cached data and also closes this instance for good.
     */
    @Override
    public void close() {
        if (gate.isOpen()) {
            log.debug("Closing");

            cleanUp();
            fileEventListener.stop();
            for (String metricName : metricNames) {
                metricRegistry.remove(metricName);
            }
            decompressedBlocks.close();
            fileTailReader.close();
            blockReader.close();

            gate.close();
            log.debug("Closed");
        }
    }

    /**
     * Simply purges the caches.
     */
    @VisibleForTesting
    public final void cleanUp() {
        if (gate.isOpen()) {
            decompressedBlocks.invalidateAll();
            fileTailReader.invalidateAll();
            blockReader.invalidateAll();

            fileReaders.invalidateAll();
            fileReaders.cleanUp();
        }
    }
}
