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
package io.jarasandha.store.filesystem.tail;

import io.jarasandha.util.io.ByteBufs;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.jarasandha.store.api.StoreException.checkCorruption;
import static io.jarasandha.util.io.ByteBufs.*;
import static io.jarasandha.util.io.Codecs.base32Ascii;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Created by ashwin.jayaprakash.
 */
public class TailCodec {
    private static final ByteBuf TAIL_MARKER = unreleasableBuffer(wrappedBuffer(
            ("¤§" + base32Ascii("TAIL") + "§¤").getBytes(US_ASCII)
    ).asReadOnly());
    private static final short TAIL_VERSION = 1;
    private static final int TAIL_SIZE_BYTES = encodedTailLength();

    private TailCodec() {
    }

    private static ByteBuf tailContentByteBuf(ByteBufAllocator allocator) {
        return allocator.buffer(
                SIZE_BYTES_BOOLEAN + SIZE_BYTES_BOOLEAN + SIZE_BYTES_LONG + SIZE_BYTES_LONG + SIZE_BYTES_INT
        );
    }

    private static ByteBuf encodeTail(ByteBufAllocator allocator, Tail tail) {
        final ByteBuf tailContent = tailContentByteBuf(allocator);
        try {
            tailContent
                    .writeBoolean(tail.isBlocksCompressed())
                    .writeBoolean(tail.isIndexCompressed())
                    .writeLong(tail.getIndexStartOffset())
                    .writeLong(tail.getIndexEndOffset())
                    .writeInt(tail.getNumBlocks());
            final long checksum = ByteBufs.calculateChecksum(tailContent.slice());
            return allocator
                    .buffer()
                    .writeBytes(TAIL_MARKER.slice())
                    .writeShort(TAIL_VERSION)
                    .writeBytes(tailContent)
                    .writeLong(checksum);
        } finally {
            tailContent.release();
        }
    }

    private static int encodedTailLength() {
        ByteBuf byteBuf = encodeTail(UnpooledByteBufAllocator.DEFAULT, new Tail(true, true, 0, 0, 0));
        try {
            return byteBuf.readableBytes();
        } finally {
            byteBuf.release();
        }
    }

    public static int sizeOfTailBytes() {
        return TAIL_SIZE_BYTES;
    }

    /**
     * @param fileChannel
     * @param allocator
     * @param tail
     * @return The number of bytes that were written.
     * @throws IOException
     */
    public static int writeTail(@CallerMustRelease FileChannel fileChannel, ByteBufAllocator allocator, Tail tail)
            throws IOException {
        final ByteBuf tailByteBuf = encodeTail(allocator, tail);
        try {
            return ByteBufs.writeChunks(fileChannel, tailByteBuf, 128);
        } finally {
            tailByteBuf.release();
        }
    }

    public static Tail readTail(@CallerMustRelease FileChannel fileChannel, ByteBufAllocator allocator)
            throws IOException {

        final long fileSizeBytes = fileChannel.size();
        checkCorruption(fileSizeBytes > TAIL_SIZE_BYTES,
                "File size should be larger than the tail size of [%d] bytes", TAIL_SIZE_BYTES);

        final ByteBuf tailBuf = allocator.buffer(TAIL_SIZE_BYTES, TAIL_SIZE_BYTES);
        try {
            final long tailStartOffset = fileSizeBytes - TAIL_SIZE_BYTES;
            tailBuf.writeBytes(fileChannel, tailStartOffset, TAIL_SIZE_BYTES);

            final int markerLength = TAIL_MARKER.readableBytes();
            final ByteBuf marker = tailBuf.readSlice(markerLength);
            checkCorruption(ByteBufUtil.compare(marker, TAIL_MARKER) == 0,
                    "The tail marker from the file does not match the expected marker");

            final short version = tailBuf.readShort();
            checkCorruption(version == TAIL_VERSION,
                    "The tail marker version number [%d] from the file does not match the expected version [%d]",
                    version, TAIL_VERSION);

            final boolean compressBlocks = tailBuf.readBoolean();
            final boolean compressIndex = tailBuf.readBoolean();
            final long indexStartOffset = tailBuf.readLong();
            final long indexEndOffset = tailBuf.readLong();
            final int numBlocks = tailBuf.readInt();
            final long expectedChecksum = tailBuf.readLong();
            checkCorruption(indexStartOffset <= indexEndOffset,
                    "indexStartOffset should be less or equals to indexEndOffset");
            checkCorruption(indexEndOffset <= tailStartOffset,
                    "indexEndOffset should be less than tailStartOffset");
            checkCorruption(numBlocks >= 0, "numBlocks should be 0 or greater");
            final ByteBuf actualChecksumByteBuf = tailContentByteBuf(allocator);
            try {
                actualChecksumByteBuf
                        .writeBoolean(compressBlocks)
                        .writeBoolean(compressIndex)
                        .writeLong(indexStartOffset)
                        .writeLong(indexEndOffset)
                        .writeInt(numBlocks);
                final long actualChecksum = ByteBufs.calculateChecksum(actualChecksumByteBuf);
                checkCorruption(expectedChecksum == actualChecksum, "tail checksums do not match");
            } finally {
                actualChecksumByteBuf.release();
            }
            return new Tail(compressBlocks, compressIndex, indexStartOffset, indexEndOffset, numBlocks);
        } finally {
            tailBuf.release();
        }
    }
}
