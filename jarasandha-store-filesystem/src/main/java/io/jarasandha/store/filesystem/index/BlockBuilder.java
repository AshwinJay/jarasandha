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
package io.jarasandha.store.filesystem.index;

import io.jarasandha.store.api.BlockWithRecordSizes;
import io.jarasandha.util.collection.MutableInts;
import io.jarasandha.util.collection.MutableIntsWrapper;
import lombok.Getter;

/**
 * Created by ashwin.jayaprakash.
 */
public class BlockBuilder {
    @Getter
    private final long blockStartByteOffset;
    @Getter
    private final MutableInts recordByteSizes;
    @Getter
    private final boolean compressed;

    BlockBuilder(long blockStartByteOffset, boolean compressed) {
        this.blockStartByteOffset = blockStartByteOffset;
        this.recordByteSizes = new MutableIntsWrapper();
        this.compressed = compressed;
    }

    public void addRecordSizeBytes(int recordByteSizeBytes) {
        this.recordByteSizes.add(recordByteSizeBytes);
    }

    /**
     * @param blockEndByteOffset This has to be provided because {@link #getBlockStartByteOffset()} +
     *                           {@link #getRecordByteSizes()} may not be equal to {@code blockEndByteOffset}.
     * @return
     */
    public BlockWithRecordSizes build(long blockEndByteOffset) {
        return Builders.buildBlockWithRecordSizes(blockStartByteOffset, blockEndByteOffset, recordByteSizes);
    }
}
