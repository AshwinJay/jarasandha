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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.jarasandha.store.api.StoreException.StoreCorruptException;
import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.TailCodec;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.stream.IntStream;

import static io.jarasandha.store.filesystem.DebugFileWriter.randomNewFileId;
import static io.jarasandha.util.collection.ImmutableBitSet.newBitSetWithAllPositionsTrue;
import static io.jarasandha.util.io.ByteBufs.write;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class FileReaderBasicTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = StoreReadException.class)
    public void testMissingFile() throws IOException {
        FileId fileId = new FileId(Uuids.newUuid(), "dat");
        File file = folder.newFile(fileId.toString());
        assertTrue(file.delete());
        assertFalse(file.exists());

        new FileReader(file, fileId, allocator, new DebugTailReader(), new DebugBlockReader());
    }

    @Test(expected = StoreCorruptException.class)
    public void testBlankFile() throws IOException {
        FileId fileId = new FileId(Uuids.newUuid(), "dat");
        File file = folder.newFile(fileId.toString());
        assertTrue(file.exists());

        new FileReader(file, fileId, allocator, new DebugTailReader(), new DebugBlockReader());
    }

    @Test(expected = StoreCorruptException.class)
    public void testJunkFile() throws IOException {
        FileId fileId = new FileId(Uuids.newUuid(), "dat");
        File file = folder.newFile(fileId.toString());
        //File some junk data.
        LinkedList<String> strings = new LinkedList<>();
        IntStream.range(0, 100).forEach(i -> strings.add(Uuids.newUuid().toString()));
        java.nio.file.Files.write(file.toPath(), strings, UTF_8, StandardOpenOption.WRITE);

        new FileReader(file, fileId, allocator, new DebugTailReader(), new DebugBlockReader());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedFileNameAndId() throws IOException {
        File file = folder.newFile();
        assertTrue(file.delete());
        try (
                FileWriter writer = new FileWriter(randomNewFileId(), file, 1024, 128,
                        allocator, new NoOpFileWriteProgressListener())
        ) {
            IntStream.range(0, 10).forEach(i -> writer.write(Uuids.newUuidByteBuf()));
        }
        FileId fileId = new FileId(Uuids.newUuid(), "dat");

        new FileReader(file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()).validate();
    }

    @Test
    public void testCorruptContent() throws IOException {
        @AllArgsConstructor
        @ToString
        class TestParams {
            //True = Index content. False = Body content.
            final boolean corruptAtEnd;
            final int corruptByteSize;
            final Class<? extends RuntimeException> exception;
            final boolean compressedBody;
            final boolean compressedIndex;
        }

        ImmutableList.<TestParams>builder()
                .add(new TestParams(true, 16, StoreCorruptException.class, false, false))
                .add(new TestParams(true, TailCodec.sizeOfTailBytes(), StoreCorruptException.class, true, false))
                .add(new TestParams(true, 16, StoreCorruptException.class, false, true))
                .add(new TestParams(true, 16, StoreCorruptException.class, true, true))
                .add(new TestParams(true, 6, StoreCorruptException.class, false, false))
                .add(new TestParams(true, 6, StoreCorruptException.class, true, false))
                .add(new TestParams(true, 6, StoreCorruptException.class, false, true))
                .add(new TestParams(true, 6, StoreCorruptException.class, true, true))
                .add(new TestParams(false, 6, StoreCorruptException.class, true, true))
                .build()
                .forEach(params -> {
                    log.info("Testing [{}]", params);

                    try {
                        FileId fileId = new FileId(Uuids.newUuid(), "dat");
                        File file = folder.newFile(fileId.toString());
                        assertTrue(file.delete());
                        try (
                                FileWriter writer = new FileWriter(randomNewFileId(), file, 1024, 128,
                                        allocator, new NoOpFileWriteProgressListener())
                        ) {
                            IntStream.range(0, 10).forEach(i -> writer.write(Uuids.newUuidByteBuf()));
                        }

                        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                            final int fileSize = Ints.checkedCast(raf.length());

                            final int junkSizeBytes = params.corruptByteSize;
                            ByteBuf byteBuf = Unpooled.buffer(junkSizeBytes);
                            try {
                                //Fill with junk.
                                IntStream.range(0, junkSizeBytes).forEach(i -> byteBuf.writeByte(127));
                                try (ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                                     ReadableByteChannel channel = Channels.newChannel(bis)) {
                                    assertEquals(junkSizeBytes, byteBuf.readableBytes());
                                    write(raf.getChannel(),
                                            (params.corruptAtEnd ? fileSize - junkSizeBytes : fileSize / 2),
                                            byteBuf.readableBytes(), channel);
                                }
                            } finally {
                                byteBuf.release();
                            }
                        }

                        try {
                            FileReader fileReader = new FileReader(
                                    file, fileId, allocator, new DebugTailReader(), new DebugBlockReader()
                            ).validate();
                            //Try to readIndex the blocks if the index was not corrupt, but maybe body is.
                            fileReader
                                    .readIndex(newBitSetWithAllPositionsTrue())
                                    .blocks()
                                    .forEach(fileReader::readBlock);
                            failIfReaches();
                        } catch (Exception e) {
                            log.debug("[" + params + "] got", e);
                            //Expected exception.
                            assertTrue(e.getClass().toString(), params.exception.equals(e.getClass()));
                        }
                    } catch (IOException e) {
                        fail("Unexpected error: " + e.getMessage());
                    }
                });
    }
}
