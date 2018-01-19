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

import com.google.common.base.MoreObjects;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.jarasandha.util.misc.CostlyOperation;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.lucene.util.packed.PackedLongValues.Builder;

import java.util.function.IntConsumer;

import static com.google.common.primitives.Ints.checkedCast;

/**
 * Created by ashwin.jayaprakash.
 */
public class PackedIntsWrapper implements Ints {
    private final PackedLongValues actual;

    /**
     * @param builder
     * @see #PackedIntsWrapper(PackedLongValues)
     */
    public PackedIntsWrapper(Builder builder) {
        this(builder.build());
    }

    /**
     * @param actual The values in this list are assumed to be ints and not longs.
     * @throws IllegalArgumentException The {@link PackedLongValues#size() size} is assumed to fit into an int (just
     *                                  like the actual values themselves).
     */
    public PackedIntsWrapper(PackedLongValues actual) {
        checkedCast(actual.size());
        this.actual = actual;
    }

    public static Builder builder() {
        return PackedLongValues.packedBuilder(PackedInts.COMPACT);
    }

    @Override
    public int size() {
        return (int) actual.size();
    }

    /**
     * @param index
     * @return The internal long value (that may or may not fit into an int) but cast and returned without checking.
     */
    @Override
    public int get(int index) {
        return (int) actual.get(index);
    }

    @Override
    public void forEach(IntConsumer intConsumer) {
        final int size = size();
        for (int i = 0; i < size; i++) {
            intConsumer.accept(get(i));
        }
    }

    @CostlyOperation
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PackedIntsWrapper)) {
            return false;
        }
        final PackedIntsWrapper that = (PackedIntsWrapper) o;
        final int size = size();
        if (size == that.size()) {
            int i = 0;
            for (; i < size && get(i) == that.get(i); i++) {
                //Match item by item.
            }
            return (i == size);
        }
        return false;
    }

    @CostlyOperation
    @Override
    public int hashCode() {
        final Hasher hasher = Hashing.adler32().newHasher();
        forEach(hasher::putInt);
        return hasher.hash().asInt();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("size", actual.size())
                .toString();
    }
}
