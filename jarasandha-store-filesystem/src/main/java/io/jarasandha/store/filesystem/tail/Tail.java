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
package io.jarasandha.store.filesystem.tail;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Created by ashwin.jayaprakash.
 */
@ToString
@Getter
@EqualsAndHashCode
public class Tail {
    private final boolean blocksCompressed;
    private final boolean indexCompressed;
    private final long indexStartOffset;
    private final long indexEndOffset;
    private final int numBlocks;

    public Tail(boolean blocksCompressed, boolean indexCompressed,
                long indexStartOffset, long indexEndOffset, int numBlocks) {
        this.blocksCompressed = blocksCompressed;
        this.indexCompressed = indexCompressed;
        this.indexStartOffset = indexStartOffset;
        this.indexEndOffset = indexEndOffset;
        this.numBlocks = numBlocks;
    }
}
