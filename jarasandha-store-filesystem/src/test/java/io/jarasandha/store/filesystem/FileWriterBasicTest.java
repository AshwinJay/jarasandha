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

import ch.qos.logback.classic.Level;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import io.jarasandha.store.api.StoreException.RecordTooBigException;
import io.jarasandha.store.api.StoreException.StoreFullException;
import io.jarasandha.store.api.StoreException.StoreNotOpenException;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import static io.jarasandha.store.filesystem.DebugFileWriter.randomNewFileId;
import static io.jarasandha.store.filesystem.FileWriter.estimateSizeOfFileBytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.collections.impl.tuple.Tuples.twin;
import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class FileWriterBasicTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public FileWriterBasicTest() {
        super(Level.INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDirExists() throws IOException {
        new FileWriter(randomNewFileId(), folder.newFolder(), 1024, 128,
                allocator, new NoOpFileWriteProgressListener());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFileExists() throws IOException {
        new FileWriter(randomNewFileId(), folder.newFile(), 1024, 128, allocator, new NoOpFileWriteProgressListener());
    }

    @Test
    public void testParamsValidation() throws IOException {
        File file = folder.newFile();
        assertTrue(file.delete());
        new FileWriter(randomNewFileId(), file, 1024, 128, allocator, new NoOpFileWriteProgressListener());

        file = folder.newFile();
        assertTrue(file.delete());
        new FileWriter(randomNewFileId(), file, 1024, 1024, allocator, new NoOpFileWriteProgressListener());

        try {
            file = folder.newFile();
            assertTrue(file.delete());
            new FileWriter(randomNewFileId(), file, 1000, 1024, allocator, new NoOpFileWriteProgressListener());
            failIfReaches();
        } catch (RuntimeException e) {
            log.debug("File size less than block size", e);
        }

        try {
            file = folder.newFile();
            assertTrue(file.delete());
            new FileWriter(
                    randomNewFileId(), file,
                    FileHeaderCodec
                            .sizeOfHeaderBytes(), FileHeaderCodec.sizeOfHeaderBytes(),
                    allocator, new NoOpFileWriteProgressListener()
            );
            failIfReaches();
        } catch (RuntimeException e) {
            log.debug("File size and block size too small", e);
        }

        try {
            file = folder.newFile();
            assertTrue(file.delete());
            new FileWriter(
                    randomNewFileId(), file, 1024, FileWriter.minSizeOfBlockBytes(0),
                    allocator, new NoOpFileWriteProgressListener()
            );
            failIfReaches();
        } catch (RuntimeException e) {
            log.debug("Block size too small", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNoWrites_OnDiskSizeCheck() {
        Lists
                .newArrayList(
                        twin(/*Blocks compress*/ false, /*Index compress*/false),
                        twin(false, true),
                        twin(true, false),
                        twin(true, true)
                )
                .stream()
                .forEach(twin -> {
                    log.info("Test block compress [{}], index compress [{}]", twin.getOne(), twin.getTwo());

                    try {
                        final File file = folder.newFile();
                        assertTrue(file.delete());
                        final FileWriter writer = new FileWriter(
                                randomNewFileId(), file, 1024, 128,
                                allocator, new NoOpFileWriteProgressListener(),
                                twin.getOne(), twin.getTwo(), WriterParameters.SIZE_CHUNK_BYTES);

                        assertEquals(0, writer.getRecordsWrittenInBlockSoFar());
                        assertFalse(writer.flushBlock());
                        writer.close();

                        try {
                            //This estimate works correctly only if index compression = false.
                            assertEquals(estimateSizeOfFileBytes(0, 0), file.length());
                            assertFalse(twin.getTwo());
                        } catch (AssertionError e) {
                            log.debug("Expected when index is compressed", e);
                            assertTrue(twin.getTwo());
                        }

                        try {
                            writer.write(Unpooled.wrappedBuffer("foobar".getBytes(UTF_8)));
                        } catch (StoreNotOpenException e) {
                            log.debug("Expected", e);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testWrite_StoreCloseThrown() throws IOException {
        final File file = folder.newFile();
        assertTrue(file.delete());
        final DebugFileWriter writer =
                new DebugFileWriter(file, 4096, 1024, allocator, new NoOpFileWriteProgressListener());

        final ListMultimap<Long, DebugLogicalRecordLocation> locationsAndRecords = LinkedListMultimap.create();
        int i = 0;
        for (; ; i++) {
            log.debug("Writing record [{}]", i);
            ByteBuf record = Uuids.newUuidByteBuf();
            try {
                DebugLogicalRecordLocation location = writer.write(record);
                locationsAndRecords.put(location.getBlockStartByteOffset(), location);
            } catch (StoreFullException e) {
                log.debug("Expected", e);
                break;
            } finally {
                record.release();
            }
        }

        //We were able to write some records.
        assertEquals(189, i);
        assertEquals(locationsAndRecords.size(), i);
        writer.close();

        //And still stay within the limit.
        assertTrue(file.length() <= writer.getFileSizeBytesLimit());
    }

    @Test
    public void testWrite_RecordFullThrown() throws IOException {
        final File file = folder.newFile();
        assertTrue(file.delete());
        final DebugFileWriter writer =
                new DebugFileWriter(file, 4096, 1024, allocator, new NoOpFileWriteProgressListener());

        final ListMultimap<Long, DebugLogicalRecordLocation> locationsAndRecords = LinkedListMultimap.create();
        final int numRecordsToWriteInLoop = 149;
        for (int i = 0; i < numRecordsToWriteInLoop; i++) {
            log.debug("Writing record [{}]", i);
            ByteBuf record = Uuids.newUuidByteBuf();
            DebugLogicalRecordLocation location = writer.write(record);
            locationsAndRecords.put(location.getBlockStartByteOffset(), location);
            record.release();
        }

        //Start with a fresh block.
        assertTrue(writer.flushBlock());
        //Create a jumbo record.
        final ByteBuf record;
        {
            LinkedList<ByteBuf> recordParts = new LinkedList<>();
            for (int i = 0; i <= writer.getUncompressedBytesPerBlockLimit() / Uuids.UUID_BYTES_SIZE; i++) {
                recordParts.add(Uuids.newUuidByteBuf());
            }
            record = Unpooled.wrappedBuffer(recordParts.toArray(new ByteBuf[recordParts.size()]));
        }
        try {
            writer.write(record);
            fail("This should've failed as even a new block cannot fit this jumbo record");
        } catch (RecordTooBigException e) {
            log.debug("Expected", e);
        } finally {
            record.release();
        }

        //We actually wrote some records except the last jumbo.
        assertEquals(locationsAndRecords.size(), numRecordsToWriteInLoop);
        writer.close();
        //And still stay within the limit.
        assertTrue(file.length() <= writer.getFileSizeBytesLimit());
    }
}
