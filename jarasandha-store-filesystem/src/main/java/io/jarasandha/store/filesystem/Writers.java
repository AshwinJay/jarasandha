/**
 * Copyright 2018 The Jarasandha.io project authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.store.filesystem;

import io.jarasandha.store.api.StoreWriteProgressListener;
import io.jarasandha.store.api.StoreWriter;
import io.jarasandha.store.api.StoreWriters;
import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.util.concurrent.Gate;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.NotThreadSafe;
import org.eclipse.collections.api.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

/**
 * This is a utility class to create a lot of related {@link FileWriter}s.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
@NotThreadSafe
public class Writers implements StoreWriters<FileId, WriterParameters> {
    private final Files files;
    private final String fileExtension;
    private final ByteBufAllocator allocator;
    private final Supplier<StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo>> listeners;
    private final Gate gate;

    /**
     * @param files         Used to create files within a directory context.
     * @param fileExtension The file extention to be used for all the files that are created and written to. The files
     *                      are written inside the directory provided by "files". The extension is the text after the
     *                      "." in the fully qualified file name.
     * @param allocator
     * @param listeners
     */
    public Writers(Files files, String fileExtension, ByteBufAllocator allocator,
                   Supplier<StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo>> listeners) {
        this.files = files;
        this.fileExtension = fileExtension;
        this.allocator = allocator;
        this.listeners = listeners;
        this.gate = new Gate();
    }

    public String getFileExtension() {
        return fileExtension;
    }

    /**
     * @return {@link Pair}s of the {@link File} and the "UUID" of the file. These are all the files under the directory.
     */
    @CallerMustRelease
    public Stream<Pair<File, String>> showAllFiles() {
        return files.showAll();
    }

    /**
     * Similar to {@link #newStoreWriter(FileId, WriterParameters)} but the destination {@link FileId} is generated
     * at random but within the directory provided by {@link Files}.
     *
     * @param parameters
     * @return
     */
    @Override
    public StoreWriter<FileId> newStoreWriter(WriterParameters parameters) {
        checkState(gate.isOpen());

        final Pair<File, FileId> pair = files.newFile(fileExtension);
        final StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener = listeners.get();
        return new FileWriter(
                pair.getTwo(), pair.getOne(),
                parameters.fileSizeBytesLimit(), parameters.uncompressedBytesPerBlockLimit(),
                allocator, listener,
                parameters.blocksCompressed(), parameters.indexCompressed(), WriterParameters.SIZE_CHUNK_BYTES
        );
    }

    /**
     * This <b>deletes</b> the existing store (if it exists) and creates a new one.
     *
     * @param fileId
     * @param parameters
     * @return
     * @throws RuntimeException If the previous store could not be deleted or if something else went wrong while
     *                          creating the store.
     */
    @Override
    public StoreWriter<FileId> newStoreWriter(FileId fileId, WriterParameters parameters) {
        checkState(gate.isOpen());

        final File file = files.toFile(fileId);
        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("The file [" + file.getAbsolutePath() + "] already exists");
            }
            try {
                files.newFile(fileId.toUUID(), fileExtension);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        final StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener = listeners.get();
        return newFileWriter(fileId, file, listener, allocator, parameters);
    }

    public static StoreWriter<FileId> newFileWriter(
            FileId fileId, File file,
            StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> listener,
            ByteBufAllocator allocator, WriterParameters parameters
    ) {
        return new FileWriter(
                fileId, file,
                parameters.fileSizeBytesLimit(), parameters.uncompressedBytesPerBlockLimit(),
                allocator, listener,
                parameters.blocksCompressed(), parameters.indexCompressed(), parameters.writeChunkSizeBytes()
        );
    }

    @Override
    public void close() {
        if (gate.isOpen()) {
            gate.close();
            log.debug("Closed");
        }
    }
}
