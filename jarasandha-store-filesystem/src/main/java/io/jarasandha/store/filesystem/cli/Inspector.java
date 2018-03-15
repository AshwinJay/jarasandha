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
package io.jarasandha.store.filesystem.cli;

import io.jarasandha.store.api.Block;
import io.jarasandha.store.api.StoreReader;
import io.jarasandha.store.filesystem.Readers;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.collection.ImmutableBitSet;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

import static io.netty.buffer.ByteBufAllocator.DEFAULT;

@Slf4j
class Inspector implements Runnable {
    private final List<File> inputFiles;

    Inspector(List<File> inputFiles) {
        this.inputFiles = inputFiles;
    }

    @Override
    public void run() {
        final StringBuilder stringBuilder = new StringBuilder("\n" +
                "File, Size, NumRecords, NumBlocks, BlocksCompressed, IndexCompressed"
        );

        for (File inputFile : inputFiles) {
            final FileId fileId = FileId.from(inputFile);
            try (StoreReader<FileId> fileReader = Readers.newFileReader(fileId, inputFile, DEFAULT)) {
                final long totalNumRecords = fileReader
                        .readIndex(ImmutableBitSet.newBitSetWithAllPositionsTrue())
                        .blocks()
                        .stream()
                        .mapToInt(Block::numRecords)
                        .sum();

                final String inspection = String.format("%n%s, %d, %d, %d, %b, %b",
                        inputFile.getAbsolutePath(), inputFile.length(), totalNumRecords, fileReader.getNumBlocks(),
                        fileReader.isBlocksCompressed(), fileReader.isIndexCompressed());
                stringBuilder.append(inspection);
            }
        }

        log.info(stringBuilder.toString());
    }
}
