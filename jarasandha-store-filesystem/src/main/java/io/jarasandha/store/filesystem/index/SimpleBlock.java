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

import com.google.common.base.MoreObjects;
import io.jarasandha.store.api.Block;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by ashwin.jayaprakash.
 */
class SimpleBlock implements Block {
    final long blockStartByteOffset;
    final long blockEndByteOffset;
    final int numRecords;
    /**
     * To speed up look ups by caching the hash.
     */
    private int hashCode;

    SimpleBlock(long blockStartByteOffset, long blockEndByteOffset, int numRecords) {
        this.blockStartByteOffset = blockStartByteOffset;
        this.blockEndByteOffset = blockEndByteOffset;
        this.numRecords = numRecords;
    }

    void validate(boolean indexCompressed) {
        checkArgument(blockStartByteOffset >= 0, "blockStartByteOffset");
        checkArgument(blockEndByteOffset >= blockStartByteOffset, "blockEndByteOffset");
        checkArgument(numRecords >= 0, "numRecords");
    }

    @Override
    public long blockStartByteOffset() {
        return blockStartByteOffset;
    }

    @Override
    public long blockEndByteOffset() {
        return blockEndByteOffset;
    }

    @Override
    public int numRecords() {
        return numRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimpleBlock)) {
            return false;
        }
        SimpleBlock block = (SimpleBlock) o;
        return blockStartByteOffset == block.blockStartByteOffset &&
                blockEndByteOffset == block.blockEndByteOffset &&
                numRecords == block.numRecords;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hash(blockStartByteOffset, blockEndByteOffset, numRecords);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("blockStartByteOffset", blockStartByteOffset)
                .add("blockEndByteOffset", blockEndByteOffset)
                .add("numRecords", numRecords)
                .toString();
    }
}
