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

import io.jarasandha.util.io.ByteBufs;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.jarasandha.store.api.StoreException.checkCorruption;
import static io.jarasandha.util.io.Codecs.base32Ascii;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Created by ashwin.jayaprakash.
 */
abstract class FileHeaderCodec {
    private static final ByteBuf HEADER_MARKER = unreleasableBuffer(wrappedBuffer(
            ("¤§" + base32Ascii("HEADER") + "§¤").getBytes(US_ASCII)
    ).asReadOnly());
    private static final short HEADER_VERSION = 1;
    private static final int HEADER_SIZE_BYTES = encodedHeaderLength();

    private FileHeaderCodec() {
    }

    private static ByteBuf encodeHeader(ByteBufAllocator allocator) {
        return allocator
                .buffer()
                .writeBytes(HEADER_MARKER.slice())
                .writeShort(HEADER_VERSION);
    }

    private static int encodedHeaderLength() {
        ByteBuf byteBuf = encodeHeader(UnpooledByteBufAllocator.DEFAULT);
        try {
            return byteBuf.readableBytes();
        } finally {
            byteBuf.release();
        }
    }

    static int sizeOfHeaderBytes() {
        return HEADER_SIZE_BYTES;
    }

    /**
     * @param fileChannel
     * @param allocator
     * @return The number of bytes that were written.
     * @throws IOException
     */
    static int writeHeader(@CallerMustRelease FileChannel fileChannel, ByteBufAllocator allocator) throws IOException {
        final ByteBuf headerByteBuf = encodeHeader(allocator);
        try {
            return ByteBufs.writeChunks(fileChannel, headerByteBuf, 128);
        } finally {
            headerByteBuf.release();
        }
    }

    static void readHeader(@CallerMustRelease FileChannel fileChannel, ByteBufAllocator allocator) throws IOException {
        checkCorruption(fileChannel.size() > HEADER_SIZE_BYTES,
                "File size should be larger than the header size of [%d] bytes", HEADER_SIZE_BYTES);

        final ByteBuf header = allocator.buffer(HEADER_SIZE_BYTES, HEADER_SIZE_BYTES);
        try {
            header.writeBytes(fileChannel, 0, HEADER_SIZE_BYTES);

            final int markerLength = HEADER_MARKER.readableBytes();
            final ByteBuf marker = header.readSlice(markerLength);
            checkCorruption(ByteBufUtil.compare(marker, HEADER_MARKER) == 0,
                    "The header from the file does not match the expected header");

            final short version = header.readShort();
            checkCorruption(version == HEADER_VERSION,
                    "The header version number [%d] from the file does not match the expected version [%d]",
                    version, HEADER_VERSION);
        } finally {
            header.release();
        }
    }
}
