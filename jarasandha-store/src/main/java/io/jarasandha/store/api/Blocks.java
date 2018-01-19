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
package io.jarasandha.store.api;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map.Entry;

/**
 * Utility methods on {@link Block}s.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public final class Blocks<B extends Block> {
    private final RangeMap<Integer, BlockRef<B>> recordPositionsAndBlocks;

    public Blocks(Index<B> index) {
        this.recordPositionsAndBlocks = build(index);
    }

    /**
     * @param index
     * @param <B>
     * @return A faster way to lookup blocks using the position of a record within the overall index
     * ({@link RangeMap}'s key).
     * Each {@link Block} has {@link Block#numRecords()} the overall index has "N" such blocks.
     * The int part of ({@link BlockRef#getBlockPosition()}) denotes the position of the {@link Block} in
     * {@link Index#blocks()}.
     */
    private static <B extends Block> RangeMap<Integer, BlockRef<B>> build(Index<B> index) {
        final TreeRangeMap<Integer, BlockRef<B>> recordPositionsAndBlocks = TreeRangeMap.create();

        int i = 0;
        int blockPosition = 0;
        for (B block : index.blocks()) {
            int j = i + block.numRecords();
            Range<Integer> range = Range.closedOpen(i, j);
            recordPositionsAndBlocks.put(range, new BlockRef<>(blockPosition, block));
            i = j;
            blockPosition++;
        }

        return recordPositionsAndBlocks;
    }

    /**
     * @param blockUnwareRecordPosition This is the logical position of the record in the {@link Index} that is
     *                                   ignorant of the fact that the index has {@link Block}s.
     * @return The block in which the record at the position exists. Null if it does not exist.
     */
    public BlockRef<B> findBlockRefForRecordPosition(int blockUnwareRecordPosition) {
        final Entry<Range<Integer>, BlockRef<B>> entry = findEntry(blockUnwareRecordPosition);
        if (entry != null) {
            return entry.getValue();
        }
        return null;
    }

    private Entry<Range<Integer>, BlockRef<B>> findEntry(int blockUnwareRecordPosition) {
        return recordPositionsAndBlocks.getEntry(blockUnwareRecordPosition);
    }

    /**
     * @param blockUnwareRecordPosition This is the logical position of the record in the {@link Index} that is
     *                                   ignorant of the fact that the index has {@link Block}s.
     * @return The translated location. Null if it does not exist.
     */
    public LogicalRecordLocation localize(int blockUnwareRecordPosition) {
        final Entry<Range<Integer>, BlockRef<B>> entry = findEntry(blockUnwareRecordPosition);
        if (entry != null) {
            final Range<Integer> range = entry.getKey();
            int blockFirstRecordStart = range.lowerEndpoint();
            if (range.lowerBoundType() == BoundType.OPEN) {
                blockFirstRecordStart++;
            }
            final int recordOffset = blockUnwareRecordPosition - blockFirstRecordStart;

            final BlockRef<B> blockRef = entry.getValue();
            final int blockPosition = blockRef.getBlockPosition();

            return new LogicalRecordLocation(blockPosition, recordOffset);
        }
        return null;
    }

    @Getter
    public static class BlockRef<B extends Block> {
        private final int blockPosition;
        private final B block;

        private BlockRef(int blockPosition, B block) {
            this.blockPosition = blockPosition;
            this.block = block;
        }
    }
}
