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
package io.jarasandha.store.filesystem.shared;

import io.jarasandha.store.api.BlockWithRecordSizes;
import io.jarasandha.store.api.StoreBlockInfo;

/**
 * Created by ashwin.jayaprakash.
 */
public class FileBlockInfo implements StoreBlockInfo {
    private final int blockPosition;
    private final BlockWithRecordSizes blockWithRecordSizes;
    private final long blockStartFilePosition;
    private final long blockEndFilePosition;

    public FileBlockInfo(
            int blockPosition, BlockWithRecordSizes blockWithRecordSizes,
            long blockStartFilePosition, long blockEndFilePosition
    ) {
        this.blockPosition = blockPosition;
        this.blockWithRecordSizes = blockWithRecordSizes;
        this.blockStartFilePosition = blockStartFilePosition;
        this.blockEndFilePosition = blockEndFilePosition;
    }

    @Override
    public int blockPosition() {
        return blockPosition;
    }

    @Override
    public BlockWithRecordSizes blockWithRecordSizes() {
        return blockWithRecordSizes;
    }

    public long blockStartFilePosition() {
        return blockStartFilePosition;
    }

    public long blockEndFilePosition() {
        return blockEndFilePosition;
    }
}
