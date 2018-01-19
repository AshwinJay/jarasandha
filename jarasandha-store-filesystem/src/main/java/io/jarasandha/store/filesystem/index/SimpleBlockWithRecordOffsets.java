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

import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.util.collection.Ints;
import lombok.EqualsAndHashCode;

import static com.google.common.base.Preconditions.*;

/**
 * Created by ashwin.jayaprakash.
 */
@EqualsAndHashCode(callSuper = true)
class SimpleBlockWithRecordOffsets extends SimpleBlock implements BlockWithRecordOffsets {
    private final Ints recordByteStartOffsets;
    private final int lastRecordByteSize;

    SimpleBlockWithRecordOffsets(long blockStartByteOffset, long blockEndByteOffset,
                                 Ints recordByteStartOffsets, int lastRecordByteSize) {
        super(blockStartByteOffset, blockEndByteOffset, recordByteStartOffsets.size());

        checkNotNull(recordByteStartOffsets, "recordByteStartOffsets");
        checkArgument(lastRecordByteSize >= 0, "lastRecordByteSize");
        this.recordByteStartOffsets = recordByteStartOffsets;
        this.lastRecordByteSize = lastRecordByteSize;
    }

    @Override
    void validate(boolean indexCompressed) {
        super.validate(indexCompressed);

        final long expectedStartOffset = indexCompressed ? 0 : blockStartByteOffset;
        final int numRecords = recordByteStartOffsets.size();
        for (int i = 0; i < numRecords; i++) {
            if (i == 0) {
                checkState(recordByteStartOffsets.get(i) == expectedStartOffset);
            } else {
                //Offsets are in increasing order.
                checkState(recordByteStartOffsets.get(i) > recordByteStartOffsets.get(i - 1));
            }
        }
        if (numRecords > 0 && !indexCompressed) {
            checkState((recordByteStartOffsets.get(numRecords - 1) + lastRecordByteSize) == blockEndByteOffset);
        }
    }

    @Override
    public Ints recordByteStartOffsets() {
        return recordByteStartOffsets;
    }

    @Override
    public int lastRecordByteSize() {
        return lastRecordByteSize;
    }
}
