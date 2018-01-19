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
package io.jarasandha.util.io;

import com.google.common.collect.Lists;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.UUID;

import static io.jarasandha.util.misc.Uuids.UUID_BYTES_SIZE;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class ByteBufsTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testUuidBytesSize() throws Exception {
        assertEquals(Uuids.UUID_BYTES_SIZE, Uuids.newUuidByteBuf().readableBytes());
        assertEquals(Uuids.UUID_BYTES_SIZE, Uuids.bytesFromUuid(UUID.randomUUID()).length);
    }

    @Test
    public void testWriteOptimized_NonSeekable_NoAlignedWrite() throws IOException {
        final int pageSize = 1024;
        final File file = folder.newFile();
        final ArrayList<Integer> writeSizes = new ArrayList<>();

        try (
                ByteBufOutputStream baos = new ByteBufOutputStream(Unpooled.buffer());
                WritableByteChannel wbcOrig = Channels.newChannel(baos)
        ) {
            //Cannot spy because the impl is a private NIO class.
            WritableByteChannel wbc = mock(WritableByteChannel.class);
            when(wbc.write(any(ByteBuffer.class)))
                    .thenAnswer(invocationOnMock -> {
                        ByteBuffer bb = (ByteBuffer) invocationOnMock.getArguments()[0];
                        int c = wbcOrig.write(bb);
                        writeSizes.add(c);
                        return c;
                    });

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase1Uuids = 10;
            for (int i = 0; i < numPhase1Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //Less than the page size.
            assertTrue(bb.readableBytes() < pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, bb.readableBytes());
            ByteBufs.writeOptimized(wbc, bb, pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, baos.writtenBytes());

            //1 single write.
            verify(wbc, times(1)).write(any(ByteBuffer.class));
        }

        final long phase1Size = file.length();
        try (
                ByteBufOutputStream baos = new ByteBufOutputStream(Unpooled.buffer());
                WritableByteChannel wbcOrig = Channels.newChannel(baos)
        ) {
            //Cannot spy because the impl is a private NIO class.
            WritableByteChannel wbc = mock(WritableByteChannel.class);
            when(wbc.write(any(ByteBuffer.class)))
                    .thenAnswer(invocationOnMock -> {
                        ByteBuffer bb = (ByteBuffer) invocationOnMock.getArguments()[0];
                        int c = wbcOrig.write(bb);
                        writeSizes.add(c);
                        return c;
                    });

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase2Uuids = 500;
            for (int i = 0; i < numPhase2Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //More than the page size.
            final long phase2Size = bb.readableBytes();
            assertTrue(phase2Size > 3 * pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase2Uuids, phase2Size);
            ByteBufs.writeOptimized(wbc, bb, pageSize);
            assertEquals(phase1Size + phase2Size, baos.writtenBytes());

            //Multiple write.
            verify(wbc, times((int) Math.ceil(phase2Size / (double) pageSize))).write(any(ByteBuffer.class));
        }

        assertEquals(
                //Notice the writes were never aligned even after the first write. They are simple page sized chunks.
                Lists.newArrayList(160, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 832),
                writeSizes
        );
    }

    @Test
    public void testWriteOptimized_Seekable_AlignedWrite() throws IOException {
        final int pageSize = 1024;
        final File file = folder.newFile();
        final ArrayList<Integer> writeSizes = new ArrayList<>();

        try (FileChannel fcOrig = FileChannel.open(file.toPath(), WRITE)) {
            FileChannel fc = mock(FileChannel.class);
            when(fc.size()).thenAnswer(invocationOnMock -> fcOrig.size());
            when(fc.write(any(ByteBuffer.class)))
                    .thenAnswer(invocationOnMock -> {
                        ByteBuffer bb = (ByteBuffer) invocationOnMock.getArguments()[0];
                        int c = fcOrig.write(bb);
                        writeSizes.add(c);
                        return c;
                    });

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase1Uuids = 10;
            for (int i = 0; i < numPhase1Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //Less than the page size.
            assertTrue(bb.readableBytes() < pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, bb.readableBytes());
            ByteBufs.writeOptimized(fc, bb, pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, fcOrig.size());

            //1 single write.
            verify(fc, times(1)).write(any(ByteBuffer.class));
        }

        final long phase1Size = file.length();
        try (FileChannel fcOrig = FileChannel.open(file.toPath(), APPEND)) {
            FileChannel fc = mock(FileChannel.class);
            when(fc.size()).thenAnswer(invocationOnMock -> fcOrig.size());
            when(fc.write(any(ByteBuffer.class)))
                    .thenAnswer(invocationOnMock -> {
                        ByteBuffer bb = (ByteBuffer) invocationOnMock.getArguments()[0];
                        int c = fcOrig.write(bb);
                        writeSizes.add(c);
                        return c;
                    });

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase2Uuids = 500;
            for (int i = 0; i < numPhase2Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //More than the page size.
            final long phase2Size = bb.readableBytes();
            assertTrue(phase2Size > 3 * pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase2Uuids, phase2Size);
            ByteBufs.writeOptimized(fc, bb, pageSize);
            assertEquals(phase1Size + phase2Size, fcOrig.size());

            //Multiple write.
            verify(fc, times((int) Math.ceil(phase2Size / (double) pageSize))).write(any(ByteBuffer.class));
        }

        assertEquals(
                //First 2 writes add up to 1 aligned page. Rest are aligned.
                Lists.newArrayList(160, 864, 1024, 1024, 1024, 1024, 1024, 1024, 992),
                writeSizes
        );
    }

    @Test
    public void testWriteFileChannel() throws IOException {
        final int pageSize = 1024;
        final File file = folder.newFile();

        try (FileChannel fcOrig = FileChannel.open(file.toPath(), WRITE)) {
            FileChannel fc = spy(fcOrig);

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase1Uuids = 10;
            for (int i = 0; i < numPhase1Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //Less than the page size.
            assertTrue(bb.readableBytes() < pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, bb.readableBytes());
            try (ReadableByteChannel rbc = Channels.newChannel(new ByteBufInputStream(bb))) {
                ByteBufs.write(fc, fc.position(), bb.readableBytes(), rbc);
            }
            assertEquals(UUID_BYTES_SIZE * numPhase1Uuids, fcOrig.size());

            verify(fc, times(1)).transferFrom(any(ReadableByteChannel.class), anyLong(), anyLong());
            //Unchanged.
            assertEquals(0, fcOrig.position());
        }

        final long phase1Size = file.length();
        try (FileChannel fcOrig = FileChannel.open(file.toPath(), WRITE)) {
            FileChannel fc = spy(fcOrig);

            final ByteBuf bb = Unpooled.buffer();
            final int numPhase2Uuids = 500;
            for (int i = 0; i < numPhase2Uuids; i++) {
                bb.writeBytes(Uuids.newUuidByteBuf());
            }
            //More than the page size.
            final long phase2Size = bb.readableBytes();
            assertTrue(phase2Size > 3 * pageSize);
            assertEquals(UUID_BYTES_SIZE * numPhase2Uuids, phase2Size);
            try (ReadableByteChannel rbc = Channels.newChannel(new ByteBufInputStream(bb))) {
                ByteBufs.write(fc, fcOrig.size(), bb.readableBytes(), rbc);
            }
            assertEquals(phase1Size + phase2Size, fcOrig.size());

            verify(fc, times(1)).transferFrom(any(ReadableByteChannel.class), anyLong(), anyLong());
            //Unchanged.
            assertEquals(0, fcOrig.position());
        }
    }
}
