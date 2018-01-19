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
package io.jarasandha.util.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * An object whose state can be written to {@link #toBytes()}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
public interface Persistable {
    default byte[] toBytes() {
        ByteBuf byteBuf = toBytes(ByteBufs.UNPOOLED_HEAP_BASED_ALLOCATOR);
        return byteBuf.array();
    }

    ByteBuf toBytes(ByteBufAllocator allocator);
}
