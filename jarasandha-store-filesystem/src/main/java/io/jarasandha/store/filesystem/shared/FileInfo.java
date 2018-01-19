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

import io.jarasandha.store.api.StoreInfo;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * Created by ashwin.jayaprakash.
 */
@Accessors(fluent = true)
public class FileInfo implements StoreInfo<FileId> {
    private final FileId fileId;
    private final boolean compressedBlocks;
    @Getter
    private final File file;
    @Getter
    private final FileChannel fileChannel;

    public FileInfo(FileId fileId, boolean compressedBlocks, File file, FileChannel fileChannel) {
        this.fileId = fileId;
        this.compressedBlocks = compressedBlocks;
        this.file = file;
        this.fileChannel = fileChannel;
    }

    @Override
    public FileId storeId() {
        return fileId;
    }

    @Override
    public boolean compressedBlocks() {
        return compressedBlocks;
    }
}
