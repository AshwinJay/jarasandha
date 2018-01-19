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
import lombok.EqualsAndHashCode;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.function.IntConsumer;

/**
 * Created by ashwin.jayaprakash.
 */
@EqualsAndHashCode
public class MutableIntsWrapper implements MutableInts {
    private final MutableIntList actual;

    public MutableIntsWrapper() {
        this(new IntArrayList());
    }

    public MutableIntsWrapper(MutableIntList actual) {
        this.actual = actual;
    }

    @Override
    public void add(int i) {
        actual.add(i);
    }

    @Override
    public int size() {
        return actual.size();
    }

    @Override
    public int get(int index) {
        return actual.get(index);
    }

    @Override
    public void forEach(IntConsumer intConsumer) {
        actual.forEach(intConsumer::accept);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("size", actual.size())
                .toString();
    }
}
