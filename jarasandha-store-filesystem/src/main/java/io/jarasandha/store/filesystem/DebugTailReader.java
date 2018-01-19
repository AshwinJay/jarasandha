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
import com.github.benmanes.caffeine.cache.Caffeine;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.FileChannel;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class DebugTailReader extends CacheAwareTailReader {
    DebugTailReader() {
        super(Caffeine.newBuilder().build(), new MetricRegistry(), "debug-tail-reader");
    }

    @Override
    public Tail read(FileId fileId, ByteBufAllocator allocator, FileChannel fileChannel) {
        Tail tail = super.read(fileId, allocator, fileChannel);
        if (log.isDebugEnabled()) {
            log.debug("File [{}] has tail [{}]", fileId, tail);
        }
        return tail;
    }
}
