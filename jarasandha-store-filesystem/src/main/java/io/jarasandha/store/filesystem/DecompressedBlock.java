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

import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * {@link ByteBuf#release()} must be called on {@link #getDecompressedBlockBytes()} before this instance is discarded.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Getter
class DecompressedBlock {
    private final BlockWithRecordOffsets block;
    private final ByteBuf decompressedBlockBytes;

    DecompressedBlock(BlockWithRecordOffsets block, ByteBuf decompressedBlockBytes) {
        this.decompressedBlockBytes = decompressedBlockBytes;
        this.block = block;
    }
}
