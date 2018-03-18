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
import io.jarasandha.store.api.Block;
import io.jarasandha.util.misc.Validateable;
import io.netty.buffer.ByteBufAllocator;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@Setter
@Accessors(fluent = true)
public class ReadersParameters implements Validateable<ReadersParameters> {
    private Files files;
    private FileEventListener fileEventListener;
    private BlockEventListener blockEventListener;
    private ByteBufAllocator allocator;
    private MetricRegistry metricRegistry;
    /**
     * Maximum time for which entries (decompressed blocks and files) will be cached.
     */
    private Duration expireAfterAccess;
    /**
     * Maximum number of files that will be kept open/cached.
     */
    private int maxOpenFiles;
    /**
     * Maximum number of {@link Block}s that will be kept open/cached.
     */
    private int maxOpenIndexBlocks;
    /**
     * The maximum total decompressed bytes that will be cached.
     */
    private long maxTotalUncompressedBlockBytes;

    @Override
    public ReadersParameters validate() {
        checkNotNull(files, "files");
        checkNotNull(fileEventListener, "fileHandler");
        checkNotNull(blockEventListener, "blockEventListener");
        checkNotNull(allocator, "allocator");
        checkNotNull(metricRegistry, "metricRegistry");
        checkNotNull(expireAfterAccess, "expireAfterAccess");
        checkArgument(expireAfterAccess.toNanos() >= 0, "expireAfterAccess");
        checkArgument(maxOpenFiles >= 0, "maxOpenFiles");
        checkArgument(maxOpenIndexBlocks >= 0, "maxOpenIndexBlocks");
        checkArgument(maxTotalUncompressedBlockBytes >= 0, "maxTotalUncompressedBlockBytes");

        return this;
    }
}
