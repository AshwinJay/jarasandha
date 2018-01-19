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

import com.google.common.base.Preconditions;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.AbstractList;
import java.util.List;

/**
 * A fixed size {@link List} that is backed by a map because the list could be sparsely populated (This is not
 * particularly good to use when the list it not sparse).
 * <p>
 * To make it less confusing, especially since the {@link #size() size} is fixed and so certain mutative operations are
 * not supported. By default, all the positions from "0" to "size - 1" are assumed to have the given default value.
 * <p>
 * Created by ashwin.jayaprakash.
 */
public class SparseFixedSizeList<V> extends AbstractList<V> {
    private final MutableIntObjectMap<V> internalMap;
    private final int fixedVirtualSize;
    private final V defaultValue;

    /**
     * @param fixedVirtualSize This is the fixed size of the list. The list may not actually contain this many items.
     * @param defaultValue     This is the value returned when there is no actual value set or was "null" at a position
     *                         in the list.
     *                         The default value itself can be null, in which case it might be hard to distinguish
     *                         between a condition where the value was {@link #set(int, Object)} or
     *                         {@link #add(int, Object) added} or was just null/default.
     */
    public SparseFixedSizeList(int fixedVirtualSize, V defaultValue) {
        Preconditions.checkArgument(fixedVirtualSize >= 0, "fixedVirtualSize has to be 0 or greater");

        this.internalMap = new IntObjectHashMap<>(Math.max(4, fixedVirtualSize / 8));
        this.fixedVirtualSize = fixedVirtualSize;
        this.defaultValue = defaultValue;
    }

    /**
     * @param v
     * @return
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean add(V v) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param index
     * @param element
     * @throws UnsupportedOperationException
     */
    @Override
    public void add(int index, V element) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param index
     * @return
     * @throws UnsupportedOperationException
     */
    @Override
    public V remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V set(int index, V element) {
        Preconditions.checkElementIndex(index, fixedVirtualSize);
        final V value;
        if (element == null || element == defaultValue) {
            value = internalMap.remove(index);
        } else {
            value = internalMap.put(index, element);
        }
        return (value == null) ? defaultValue : value;
    }

    @Override
    public V get(int index) {
        Preconditions.checkElementIndex(index, fixedVirtualSize);
        V value = internalMap.get(index);
        return (value == null) ? defaultValue : value;
    }

    @Override
    public int size() {
        return fixedVirtualSize;
    }
}
