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
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.util.io.MoreCloseables;
import io.jarasandha.util.io.ReleasableByteBufs;
import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

import static io.jarasandha.util.misc.Formatters.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
class CacheAwareDecompressedBlocks implements AutoCloseable {
    private final Cache<FileBlockPosition, DecompressedBlock> decompressedBlocks;
    private final MetricRegistry metricRegistry;
    private final String metricsFamily;
    @Getter(AccessLevel.PACKAGE)
    private final AtomicLong decompressedBytesInCache;

    CacheAwareDecompressedBlocks(ReadersParameters parameters, String metricsFamily) {
        this.decompressedBytesInCache = new AtomicLong();

        final BlockEventListener blockEventListener = parameters.blockEventListener();
        this.decompressedBlocks =
                Caffeine.<FileBlockPosition, ByteBuf>newBuilder()
                        .expireAfterAccess(parameters.expireAfterAccess().toNanos(), NANOSECONDS)
                        .recordStats(() -> new CacheMetrics(parameters.metricRegistry(), metricsFamily))
                        .removalListener(
                                (RemovalListener<FileBlockPosition, DecompressedBlock>) (key, value, removalCause) -> {
                                    if (value != null) {
                                        final int readableBytes = value.getDecompressedBlockBytes().readableBytes();
                                        if (key != null && log.isDebugEnabled()) {
                                            log.debug("Closing block [{}] of file [{}] with [{}] bytes. Cause [{}]",
                                                    key.getBlockPosition(), key.toFileId(), format(readableBytes),
                                                    removalCause);
                                        }
                                        MoreCloseables
                                                .close(new ReleasableByteBufs(value.getDecompressedBlockBytes()), log);
                                        this.decompressedBytesInCache.addAndGet(-readableBytes);
                                    }
                                    blockEventListener.onBlockRemove(key);
                                })
                        .maximumWeight(parameters.maxTotalUncompressedBlockBytes())
                        .weigher(
                                (key, decompressedBlock)
                                        -> decompressedBlock.getDecompressedBlockBytes().readableBytes()
                        )
                        .build();

        this.metricRegistry = parameters.metricRegistry();
        this.metricsFamily = metricsFamily;
    }

    /**
     * @param fileBlockPosition
     * @return The {@link ByteBuf} that has been {@link ReferenceCounted#retain() retained} so it is important that
     * {@link ByteBuf#release()} be called on {@link DecompressedBlock#getDecompressedBlockBytes()} after use.
     * Or, null if it was not in the cache or had expired.
     */
    DecompressedBlock slice(FileBlockPosition fileBlockPosition) {
        final DecompressedBlock decompressedBlock = decompressedBlocks.getIfPresent(fileBlockPosition);
        if (decompressedBlock != null) {
            try {
                ByteBuf slicedByteBuf = decompressedBlock.getDecompressedBlockBytes().retainedSlice();
                return new DecompressedBlock(decompressedBlock.getBlock(), slicedByteBuf);
            } catch (IllegalReferenceCountException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Block [{}] of file [{}] in the cache has already expired [{}]",
                            fileBlockPosition.getBlockPosition(), fileBlockPosition.toFileId(), e.getMessage());
                }
            }
        }
        return null;
    }

    void save(FileBlockPosition fileBlockPosition, DecompressedBlock decompressedBlock) {
        decompressedBlocks.put(fileBlockPosition, decompressedBlock);
        decompressedBytesInCache.addAndGet(decompressedBlock.getDecompressedBlockBytes().readableBytes());
    }

    void invalidateAll() {
        decompressedBlocks.invalidateAll();
        decompressedBlocks.cleanUp();
    }

    @Override
    public void close() {
        invalidateAll();
        metricRegistry.remove(metricsFamily);
    }
}
