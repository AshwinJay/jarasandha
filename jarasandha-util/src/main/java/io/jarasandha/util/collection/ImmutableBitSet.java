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
package io.jarasandha.util.collection;

import lombok.EqualsAndHashCode;

import java.util.BitSet;

/**
 * Created by ashwin.jayaprakash.
 */
public interface ImmutableBitSet {
    int POSITION_NOT_SET = -1;

    /**
     * @param fromPosition The next position that is set, starting (inclusive) from the given position. So, test if a
     *                     position is set or not "{@code boolean present = (i == immutableBitSet.nextSetBit(i))}" or
     *                     simply use call {@link #get(int)}.
     * @return {@value #POSITION_NOT_SET} if there is none.
     * @see BitSet#nextSetBit(int)
     */
    int nextSetBit(int fromPosition);

    default boolean get(int position) {
        return position == nextSetBit(position);
    }

    static ImmutableBitSet newBitSetWithAllPositionsTrue() {
        return new AllTrueBitSet();
    }

    static ImmutableBitSet newBitSet(int... truePositions) {
        BitSet bitSet = new BitSet();
        for (int i : truePositions) {
            bitSet.set(i);
        }
        return newBitSet(bitSet);
    }

    static ImmutableBitSet newBitSet(BitSet truePositions) {
        return new SelectiveBitSet(truePositions);
    }

    @EqualsAndHashCode
    class SelectiveBitSet implements ImmutableBitSet {
        private final BitSet bitSet;

        private SelectiveBitSet(BitSet bitSet) {
            this.bitSet = bitSet;
        }

        @Override
        public int nextSetBit(int fromPosition) {
            return bitSet.nextSetBit(fromPosition);
        }
    }

    /**
     * All bits are set. This implementation does not store any state.
     */
    @EqualsAndHashCode
    class AllTrueBitSet implements ImmutableBitSet {
        private AllTrueBitSet() {
        }

        @Override
        public int nextSetBit(int fromPosition) {
            return fromPosition;
        }
    }
}
