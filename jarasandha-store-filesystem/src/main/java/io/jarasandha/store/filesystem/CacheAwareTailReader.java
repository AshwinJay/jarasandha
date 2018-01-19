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
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.store.filesystem.tail.TailReader;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;

import java.nio.channels.FileChannel;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
@Slf4j
class CacheAwareTailReader implements TailReader, AutoCloseable {
    private final Cache<FileId, Tail> fileTails;
    private final MetricRegistry metricRegistry;
    private final String metricsFamily;

    CacheAwareTailReader(FileReadersParameters parameters, String metricsFamily) {
        this(
                Caffeine
                        .<FileId, Tail>newBuilder()
                        .expireAfterAccess(parameters.expireAfterAccess().toNanos(), NANOSECONDS)
                        .maximumSize(parameters.maxOpenFiles())
                        .recordStats(() -> new CacheMetrics(parameters.metricRegistry(), metricsFamily))
                        .removalListener((RemovalListener<FileId, Tail>) (key, value, cause) -> {
                            if (key != null && log.isDebugEnabled()) {
                                log.debug("Removing tail [{}] from cache. Cause [{}]", key, cause);
                            }
                        })
                        .build(),
                parameters.metricRegistry(), metricsFamily
        );
    }

    CacheAwareTailReader(Cache<FileId, Tail> fileTails, MetricRegistry metricRegistry, String metricsFamily) {
        this.fileTails = fileTails;
        this.metricRegistry = metricRegistry;
        this.metricsFamily = metricsFamily;
    }

    @Override
    public Tail read(FileId fileId, ByteBufAllocator allocator, FileChannel fileChannel) {
        return fileTails.get(fileId, fileIdOnMiss -> TailReader.super.read(fileId, allocator, fileChannel));
    }

    void invalidate(FileId fileId) {
        fileTails.invalidate(fileId);
        fileTails.cleanUp();
    }

    void invalidateAll() {
        fileTails.invalidateAll();
        fileTails.cleanUp();
    }

    @Override
    public void close() {
        invalidateAll();
        metricRegistry.remove(metricsFamily);
    }
}
