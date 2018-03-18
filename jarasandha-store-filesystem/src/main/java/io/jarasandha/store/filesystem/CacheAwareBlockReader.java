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

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.filesystem.index.BlockReader;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;

import java.nio.channels.FileChannel;
import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
@Slf4j
class CacheAwareBlockReader implements BlockReader, AutoCloseable {
    private final Cache<FileBlockPosition, BlockWithRecordOffsets> indexBlocks;
    private final MetricRegistry metricRegistry;
    private final String metricsFamily;

    CacheAwareBlockReader(ReadersParameters parameters, String metricsFamily) {
        this(
                Caffeine
                        .<FileBlockPosition, BlockWithRecordOffsets>newBuilder()
                        .expireAfterAccess(parameters.expireAfterAccess().toNanos(), NANOSECONDS)
                        .maximumSize(parameters.maxOpenIndexBlocks())
                        .recordStats(() -> new CacheMetrics(parameters.metricRegistry(), metricsFamily))
                        .removalListener(
                                (RemovalListener<FileBlockPosition, BlockWithRecordOffsets>) (key, value, cause) -> {
                                    if (key != null && log.isDebugEnabled()) {
                                        log.debug("Removing block [{}] from cache. Cause [{}]", key, cause);
                                    }
                                }
                        )
                        .build(),
                parameters.metricRegistry(), metricsFamily
        );
    }

    CacheAwareBlockReader(
            Cache<FileBlockPosition, BlockWithRecordOffsets> indexBlocks,
            MetricRegistry metricRegistry, String metricsFamily
    ) {
        this.indexBlocks = indexBlocks;
        this.metricRegistry = metricRegistry;
        this.metricsFamily = metricsFamily;
    }

    /**
     * Retrieves from cache if available. Otherwise reads from the file, updates the cache before returning it.
     *
     * @param allocator
     * @param fileChannel
     * @param tail
     * @param blockPosition
     * @return
     */
    @Override
    public BlockWithRecordOffsets read(
            ByteBufAllocator allocator, @CallerMustRelease FileChannel fileChannel, Tail tail,
            FileBlockPosition blockPosition
    ) {

        BlockWithRecordOffsets block = indexBlocks.getIfPresent(blockPosition);
        if (block == null) {
            block = BlockReader.super.read(allocator, fileChannel, tail, blockPosition);
        }
        return block;
    }

    /**
     * Does not readIndex from the cache. Instead it re-reads the index from the file and then updates the cache with
     * the {@link BlockWithRecordOffsets} before returning.
     *
     * @param allocator
     * @param fileChannel
     * @param tail
     * @param fileId
     * @param blocksToRead
     * @return
     */
    @Override
    @CostlyOperation
    public Index<? extends BlockWithRecordOffsets> read(
            ByteBufAllocator allocator, FileChannel fileChannel, Tail tail,
            FileId fileId, ImmutableBitSet blocksToRead
    ) {

        final Index<? extends BlockWithRecordOffsets> index =
                readUncachedIndex(allocator, fileChannel, tail, fileId, blocksToRead);

        //Force the blocks into the cache.
        final List<? extends BlockWithRecordOffsets> blocks = index.blocks();
        final int numBlocks = blocks.size();
        for (int i = 0; i < numBlocks; ) {
            int next = blocksToRead.nextSetBit(i);
            if (next == ImmutableBitSet.POSITION_NOT_SET) {
                break;
            } else if (i == next) {
                BlockWithRecordOffsets block = blocks.get(i);
                indexBlocks.put(new FileBlockPosition(fileId, i), block);
                //Try the next one.
                i++;
            } else {
                //Skip forward.
                i = next;
            }
        }

        return index;
    }

    @VisibleForTesting
    Index<? extends BlockWithRecordOffsets> readUncachedIndex(
            ByteBufAllocator allocator, FileChannel fileChannel,
            Tail tail, FileId fileId, ImmutableBitSet blocksToRead
    ) {
        return BlockReader.super.read(allocator, fileChannel, tail, fileId, blocksToRead);
    }

    void invalidate(FileBlockPosition location) {
        indexBlocks.invalidate(location);
        indexBlocks.cleanUp();
    }

    void invalidateAll() {
        indexBlocks.invalidateAll();
        indexBlocks.cleanUp();
    }

    @Override
    public void close() {
        invalidateAll();
        metricRegistry.remove(metricsFamily);
    }
}
