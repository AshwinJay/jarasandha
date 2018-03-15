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

import com.google.common.annotations.VisibleForTesting;
import io.jarasandha.store.api.StoreWriteProgressListener;
import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.Uuids;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.File;

/**
 * Created by ashwin.jayaprakash.
 */
@VisibleForTesting
class DebugFileWriter extends FileWriter {
    private static final String TEST = ".test";

    DebugFileWriter(File file, long fileSizeBytesLimit, int uncompressedBytesPerBlockLimit,
                    ByteBufAllocator allocator, StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener,
                    boolean compressBlocks, boolean compressIndex) {
        super(
                randomNewFileId(), file,
                fileSizeBytesLimit, uncompressedBytesPerBlockLimit,
                allocator, listener,
                compressBlocks, compressIndex, WriterParameters.SIZE_CHUNK_BYTES
        );
    }

    DebugFileWriter(File file, long fileSizeBytesLimit, int uncompressedBytesPerBlockLimit,
                    ByteBufAllocator allocator, StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener) {
        super(
                randomNewFileId(), file,
                fileSizeBytesLimit, uncompressedBytesPerBlockLimit,
                allocator, listener
        );
    }

    static FileId randomNewFileId() {
        return new FileId(Uuids.newUuid(), TEST);
    }

    @Override
    public DebugLogicalRecordLocation write(@CallerMustRelease ByteBuf value) {
        return (DebugLogicalRecordLocation) super.write(value);
    }

    @Override
    DebugLogicalRecordLocation newLlr(int positionOfBlock, int positionOfRecordInBlock,
                                      long byteOffsetOfBlockStart, long byteOffsetOfRecordInBlock,
                                      int sizeOfRecordBytes) {
        return new DebugLogicalRecordLocation(positionOfBlock, positionOfRecordInBlock,
                byteOffsetOfBlockStart, byteOffsetOfRecordInBlock, sizeOfRecordBytes);
    }
}
