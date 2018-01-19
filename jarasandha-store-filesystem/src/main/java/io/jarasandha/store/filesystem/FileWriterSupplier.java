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

import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.ByteSize;
import io.jarasandha.util.misc.CallerMustRelease;
import net.jcip.annotations.NotThreadSafe;
import org.eclipse.collections.api.tuple.Pair;

import java.io.File;
import java.util.stream.Stream;

import static com.google.common.primitives.Ints.checkedCast;

/**
 * Created by ashwin.jayaprakash.
 */
@NotThreadSafe
public class FileWriterSupplier implements AutoCloseable {
    private final FileWriters writers;
    private final ByteSize fileSizeLimit;
    private final ByteSize uncompressedBlockSizeLimit;
    private final boolean blocksCompressed;
    private final boolean indexCompressed;

    public FileWriterSupplier(
            FileWriters writers,
            ByteSize fileSizeLimit,
            ByteSize uncompressedBlockSizeLimit,
            boolean blocksCompressed,
            boolean indexCompressed
    ) {
        this.writers = writers;
        this.fileSizeLimit = fileSizeLimit;
        this.uncompressedBlockSizeLimit = uncompressedBlockSizeLimit;
        this.blocksCompressed = blocksCompressed;
        this.indexCompressed = indexCompressed;
    }

    public String getFileExtension() {
        return writers.getFileExtension();
    }

    @CallerMustRelease
    public Stream<Pair<File, String>> showAllFiles() {
        return writers.showAllFiles();
    }

    public FileWriter newFileWriter() {
        final int uncompressedBytesPerBlockLimit = checkedCast(uncompressedBlockSizeLimit.toBytes());
        FileWriterParameters parameters = new FileWriterParameters()
                .fileSizeBytesLimit(fileSizeLimit.toBytes())
                .uncompressedBytesPerBlockLimit(uncompressedBytesPerBlockLimit)
                .indexCompressed(indexCompressed)
                .blocksCompressed(blocksCompressed);
        return writers.newStoreWriter(parameters);
    }

    public FileWriter newFileWriter(FileId useThisId) {
        final int uncompressedBytesPerBlockLimit = checkedCast(uncompressedBlockSizeLimit.toBytes());
        FileWriterParameters parameters = new FileWriterParameters()
                .fileSizeBytesLimit(fileSizeLimit.toBytes())
                .uncompressedBytesPerBlockLimit(uncompressedBytesPerBlockLimit)
                .indexCompressed(indexCompressed)
                .blocksCompressed(blocksCompressed);
        return writers.newStoreWriter(useThisId, parameters);
    }

    @Override
    public void close() {
        writers.close();
    }
}
