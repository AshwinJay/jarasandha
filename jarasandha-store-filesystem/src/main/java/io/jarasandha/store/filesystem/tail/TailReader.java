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
package io.jarasandha.store.filesystem.tail;

import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBufAllocator;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
public interface TailReader {
    default Tail read(FileId fileId, ByteBufAllocator allocator, @CallerMustRelease FileChannel fileChannel) {
        try {
            return TailCodec.readTail(fileChannel, allocator);
        } catch (IOException e) {
            throw new StoreReadException("Error occurred while attempting to readIndex file tail", e);
        }
    }
}
