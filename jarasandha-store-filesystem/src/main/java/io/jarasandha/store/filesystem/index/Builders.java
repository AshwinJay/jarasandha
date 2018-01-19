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

import io.jarasandha.store.api.Block;
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.BlockWithRecordSizes;
import io.jarasandha.store.api.Index;
import io.jarasandha.util.collection.Ints;
import io.jarasandha.util.collection.PackedIntsWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.util.packed.PackedLongValues.Builder;

import java.util.List;
import java.util.function.IntConsumer;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public abstract class Builders {
    private Builders() {
    }

    public static BlockBuilder blockBuilder(long blockStartByteOffset, boolean compressed) {
        return new BlockBuilder(blockStartByteOffset, compressed);
    }

    public static BlockWithRecordSizes buildBlockWithRecordSizes(long blockStartByteOffset,
                                                                 long blockEndByteOffset,
                                                                 Ints recordSizesInBlock) {

        final PackedIntsWrapper packedRecordByteSizes;
        if (recordSizesInBlock instanceof PackedIntsWrapper) {
            packedRecordByteSizes = (PackedIntsWrapper) recordSizesInBlock;
        } else {
            final Builder packedLongsBuilder = PackedIntsWrapper.builder();
            recordSizesInBlock.forEach(packedLongsBuilder::add);
            packedRecordByteSizes = new PackedIntsWrapper(packedLongsBuilder);
        }
        return new SimpleBlockWithRecordSizes(blockStartByteOffset, blockEndByteOffset, packedRecordByteSizes);
    }

    public static BlockWithRecordOffsets buildBlockWithRecordOffsets(long blockStartByteOffset,
                                                                     long blockEndByteOffset,
                                                                     Ints recordSizesInBlock) {

        final Builder recordOffsetsBuilder = PackedIntsWrapper.builder();
        final int[] lastRecordSizeSeen = new int[1];
        final IntConsumer intConsumer = new IntConsumer() {
            long cumulativeSum = 0;

            @Override
            public void accept(int recordSizeBytes) {
                recordOffsetsBuilder.add(cumulativeSum);
                cumulativeSum += recordSizeBytes;
                lastRecordSizeSeen[0] = recordSizeBytes;
            }
        };
        recordSizesInBlock.forEach(intConsumer);
        final int lastRecordByteSize = recordSizesInBlock.size() > 0 ? lastRecordSizeSeen[0] : 0;
        final PackedIntsWrapper recordOffsetsWrapper = new PackedIntsWrapper(recordOffsetsBuilder);
        if (log.isDebugEnabled()) {
            log.debug("Block start offset [{}], end [{}] and [{}] records uses [{}] bytes for its offsets",
                    blockStartByteOffset, blockEndByteOffset,
                    recordSizesInBlock.size(), recordOffsetsBuilder.ramBytesUsed());
        }
        return new SimpleBlockWithRecordOffsets
                (blockStartByteOffset, blockEndByteOffset, recordOffsetsWrapper, lastRecordByteSize);
    }

    /**
     * Converts the record offsets to record sizes.
     *
     * @param blockWithRecordOffsets
     * @param recordSizeConsumer
     */
    public static void convertToRecordSizes(BlockWithRecordOffsets blockWithRecordOffsets,
                                            IntConsumer recordSizeConsumer) {
        final Ints offsets = blockWithRecordOffsets.recordByteStartOffsets();
        final int size = offsets.size();
        for (int i = 0; i < size; i++) {
            int recordSize = convertToRecordSize(blockWithRecordOffsets, i);
            recordSizeConsumer.accept(recordSize);
        }
    }

    public static int convertToRecordSize(BlockWithRecordOffsets blockWithRecordOffsets, int at) {
        final Ints recordOffsets = blockWithRecordOffsets.recordByteStartOffsets();
        final int lastRecordByteSize = blockWithRecordOffsets.lastRecordByteSize();

        int start = recordOffsets.get(at);
        int end = (at == recordOffsets.size() - 1) ? (start + lastRecordByteSize) : recordOffsets.get(at + 1);
        return end - start;
    }

    public static <B extends Block> Index<B> buildIndex(List<? extends B> blocks) {
        return new SimpleIndex<>(blocks);
    }
}
