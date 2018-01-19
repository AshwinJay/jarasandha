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
import io.jarasandha.util.collection.Ints;
import lombok.EqualsAndHashCode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ashwin.jayaprakash.
 */
@EqualsAndHashCode(callSuper = true)
class SimpleBlockWithRecordSizes extends SimpleBlock implements BlockWithRecordSizes {
    private final Ints recordByteSizes;

    SimpleBlockWithRecordSizes(long blockStartByteOffset, long blockEndByteOffset, Ints recordByteSizes) {
        super(blockStartByteOffset, blockEndByteOffset, recordByteSizes.size());

        checkNotNull(recordByteSizes, "recordByteSizes");
        this.recordByteSizes = recordByteSizes;
    }

    @Override
    void validate(boolean indexCompressed) {
        super.validate(indexCompressed);

        if (!indexCompressed) {
            final long[] sum = new long[1];
            recordByteSizes.forEach(each -> sum[0] += each);
            //Ensure that the calculated and actual sizes are the same.
            final long actual = (blockEndByteOffset - blockStartByteOffset);
            checkArgument(sum[0] == actual,
                    "The sum of recordByteSizes [%s] does not match the actual size [%s]", sum[0], actual);
        }
    }

    @Override
    public Ints recordByteSizes() {
        return recordByteSizes;
    }
}
