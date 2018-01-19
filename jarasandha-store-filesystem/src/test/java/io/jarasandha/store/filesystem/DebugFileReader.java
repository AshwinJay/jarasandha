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
package io.jarasandha.store.filesystem;

import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.filesystem.index.BlockReader;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.TailReader;
import io.netty.buffer.ByteBufAllocator;

import java.io.File;

/**
 * Created by ashwin.jayaprakash.
 */
class DebugFileReader extends FileReader {
    DebugFileReader(
            File file, FileId fileId, ByteBufAllocator allocator,
            TailReader tailReader, BlockReader blockReader
    ) {
        super(file, fileId, allocator, tailReader, blockReader);
    }

    @Override
    public DebugFileReader validate() {
        super.validate();
        return this;
    }

    @Override
    FileRecordLocation decorateFileRecordLocation(
            File file, FileId fileId, BlockWithRecordOffsets block, FileRecordLocation original) {

        return new DebugFileRecordLocation(file, fileId, block,
                original.getRecordStartByteOffsetInBlock(), original.getRecordSizeBytes());
    }
}
