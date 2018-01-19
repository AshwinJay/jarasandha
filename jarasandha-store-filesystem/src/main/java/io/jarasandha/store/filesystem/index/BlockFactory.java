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
import io.jarasandha.util.collection.Ints;
import io.jarasandha.util.collection.MutableIntsWrapper;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

/**
 * Created by ashwin.jayaprakash.
 */
public interface BlockFactory<B extends Block> {
    int OFFSET_UNKNOWN = -1;

    B make(long blockStartByteOffset, long blockEndByteOffset, Ints recordSizesInBlock);

    /**
     * @return {@link #make(long, long, Ints)} with {@link #OFFSET_UNKNOWN}.
     */
    default B makeSkipped() {
        return make(OFFSET_UNKNOWN, OFFSET_UNKNOWN, new MutableIntsWrapper(new IntArrayList(0)));
    }
}
