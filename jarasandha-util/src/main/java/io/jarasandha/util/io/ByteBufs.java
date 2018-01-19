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

import com.google.common.annotations.VisibleForTesting;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Ints.checkedCast;
import static java.lang.Math.min;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public abstract class ByteBufs {
    public static final int SIZE_BYTES_BOOLEAN = 1;
    public static final int SIZE_BYTES_SHORT = Short.SIZE / 8;
    public static final int SIZE_BYTES_INT = Integer.SIZE / 8;
    public static final int SIZE_BYTES_LONG = SIZE_BYTES_INT * 2;
    public static final int SIZE_BYTES_CHECKSUM = SIZE_BYTES_LONG;

    public static final ByteBufAllocator UNPOOLED_HEAP_BASED_ALLOCATOR = new UnpooledByteBufAllocator(false);

    private ByteBufs() {
    }

    /**
     * Memory-maps the given {@link FileChannel} and then wraps the mapped region with {@link ByteBuf} and then
     * {@link Consumer#accept(Object) invokes} the "action" with it.
     * <p>
     * The region is unmapped before exiting from this method and the {@link ByteBuf#release()} invoked too.
     *
     * @param fileChannel
     * @param position
     * @param size
     * @param action      The mapped region is provided to this.
     * @throws IOException
     */
    public static <R> R mapAndApply(
            FileChannel fileChannel, long position, long size, Function<ByteBuf, R> action
    ) throws IOException {

        final MappedByteBuffer indexByteBuffer = fileChannel.map(MapMode.READ_ONLY, position, size);
        try {
            final ByteBuf allIndexBlocksByteBuf = Unpooled.wrappedBuffer(indexByteBuffer);
            try {
                return action.apply(allIndexBlocksByteBuf);
            } finally {
                allIndexBlocksByteBuf.release();
            }
        } finally {
            PlatformDependent.freeDirectBuffer(indexByteBuffer);
        }
    }

    /**
     * An optimized form of {@link #writeChunks(WritableByteChannel, ByteBuf, int)} where data is written in chunks
     * that are aligned at the given size.
     * <p>
     * The first chunked write may not be aligned if there already is content in the channel.
     * <p>
     * Same with the last chunked write if it does not fill the entire chunk size.
     * <p>
     * If destination is a {@link FileChannel} then use {@link #write(FileChannel, long, long, ReadableByteChannel)}.
     *
     * @param destination
     * @param data
     * @param pow2ChunkWriteSizeBytes Has to be a power of 2 (typically the page size).
     * @return Number of bytes that were written.
     * @throws IOException
     */
    public static int writeOptimized(@CallerMustRelease WritableByteChannel destination,
                                     @CallerMustRelease ByteBuf data,
                                     final int pow2ChunkWriteSizeBytes) throws IOException {
        checkArgument(pow2ChunkWriteSizeBytes > 0, "pow2ChunkWriteSizeBytes should be a positive int");
        checkArgument((pow2ChunkWriteSizeBytes & (pow2ChunkWriteSizeBytes - 1)) == 0,
                "pow2ChunkWriteSizeBytes is not a power of 2");

        int numBytesWritten = 0;
        if (destination instanceof SeekableByteChannel) {
            final int readableBytes = data.readableBytes();
            final long destinationSizeBeforeWrite = ((SeekableByteChannel) destination).size();
            //Check if the unaligned bytes to be written are manageable.
            final int unalignedBytesStickingOut =
                    checkedCast(destinationSizeBeforeWrite & (pow2ChunkWriteSizeBytes - 1));
            final int bytesToWriteBeforeAlignment = pow2ChunkWriteSizeBytes - unalignedBytesStickingOut;

            //There are some bytes that are not aligned and there is enough data that can be written after the first
            //unaligned write.
            if (unalignedBytesStickingOut > 0 && readableBytes >= bytesToWriteBeforeAlignment) {
                final ByteBuf unalignedData = data.slice(data.readerIndex(), bytesToWriteBeforeAlignment);
                final int unalignedBytesWritten = writeChunks(destination, unalignedData, bytesToWriteBeforeAlignment);
                checkState(unalignedBytesWritten == bytesToWriteBeforeAlignment);
                //Ensure that destination size is now aligned. So, further writes will automatically be aligned.
                checkState((((SeekableByteChannel) destination).size() & (pow2ChunkWriteSizeBytes - 1)) == 0);

                final ByteBuf alignedData = data.slice(unalignedBytesWritten, readableBytes - unalignedBytesWritten);
                //The remaining data can be written in aligned chunks, with the exception of the last chunk, which
                //may not have sufficient bytes to fill the chunk.
                final int alignedBytesWritten = writeChunks(destination, alignedData, pow2ChunkWriteSizeBytes);
                numBytesWritten += unalignedBytesWritten + alignedBytesWritten;
                checkState(readableBytes == numBytesWritten);

                return numBytesWritten;
            }
        }

        //Simple, default write path. No alignment attempt.
        return writeChunks(destination, data, pow2ChunkWriteSizeBytes);
    }

    /**
     * @param destination
     * @param data
     * @param chunkWriteSizeBytes
     * @return The number of bytes that were written.
     * @throws IOException
     */
    public static int writeChunks(@CallerMustRelease WritableByteChannel destination,
                                  @CallerMustRelease ByteBuf data,
                                  final int chunkWriteSizeBytes) throws IOException {
        checkArgument(chunkWriteSizeBytes > 0, "chunkWriteSizeBytes");

        final int maxReadableBytes = data.readableBytes();
        int bytesWritten = 0;
        for (; bytesWritten < maxReadableBytes; ) {
            ByteBuffer chunk = data.nioBuffer(bytesWritten, min(chunkWriteSizeBytes, maxReadableBytes - bytesWritten));
            for (; chunk.remaining() > 0; ) {
                bytesWritten += destination.write(chunk);
            }
        }

        checkState(maxReadableBytes == bytesWritten);
        return bytesWritten;
    }

    /**
     * @param destination       Does not change the {@link FileChannel#position()}.
     * @param positionToWriteTo
     * @param numBytesToWrite
     * @param source
     * @return The number of bytes that were written.
     * @throws IOException
     */
    public static int write(@CallerMustRelease FileChannel destination,
                            final long positionToWriteTo, final long numBytesToWrite,
                            @CallerMustRelease ReadableByteChannel source) throws IOException {
        checkArgument(positionToWriteTo >= 0, "positionToWriteTo");
        checkArgument(numBytesToWrite > 0, "numBytesToWrite");

        int bytesWritten = 0;
        for (long position = positionToWriteTo; bytesWritten < numBytesToWrite; ) {
            final long c = destination.transferFrom(source, position, numBytesToWrite - bytesWritten);
            position += c;
            bytesWritten += c;
        }

        checkState(numBytesToWrite == bytesWritten);
        return bytesWritten;
    }

    public static ByteBuf compress(ByteBufAllocator allocator, @CallerMustRelease ByteBuf decompressed) {
        final int decompressedByteSize = decompressed.readableBytes();
        final ByteBuf compressed = Snappy.compress(allocator, decompressed);
        //It is possible for the compressed size to be larger than the original size.
        log.trace("The decompressed size was [{}] bytes and compressed is [{}] bytes",
                decompressedByteSize, compressed.readableBytes());
        return compressed;
    }

    public static ByteBuf decompress(ByteBufAllocator allocator, @CallerMustRelease ByteBuf compressed) {
        final int compressedByteSize = compressed.readableBytes();
        final ByteBuf decompressed = Snappy.decompress(allocator, compressed);
        log.trace("The compressed size was [{}] bytes and decompressed is [{}] bytes",
                compressedByteSize, decompressed.readableBytes());
        return decompressed;
    }

    /**
     * @param givenByteBuf
     * @return
     * @see #SIZE_BYTES_CHECKSUM
     */
    public static long calculateChecksum(@CallerMustRelease ByteBuf givenByteBuf) {
        if (givenByteBuf.isDirect() && givenByteBuf.nioBufferCount() == 1) {
            return calculateChecksumNio(givenByteBuf);
        } else {
            final ByteBuf byteBuf = givenByteBuf.asReadOnly();
            final CRC32 crc32 = new CRC32();
            final byte[] bytes = new byte[Math.min(1024, byteBuf.readableBytes())];

            //Copy in chunks as CRC does not support Netty bytebuf.
            for (int readableByteCount; (readableByteCount = byteBuf.readableBytes()) > 0; ) {
                final int numBytesToRead = Math.min(bytes.length, readableByteCount);
                byteBuf.readBytes(bytes, 0, numBytesToRead);
                crc32.update(bytes, 0, numBytesToRead);
            }

            return crc32.getValue();
        }
    }

    @CostlyOperation
    @VisibleForTesting
    static long calculateChecksumNio(ByteBuf directSingleNioBuf) {
        final CRC32 crc32 = new CRC32();
        //This copies the contents to a new buffer if there are composites etc.
        final ByteBuffer byteBuffer = directSingleNioBuf.nioBuffer();
        crc32.update(byteBuffer);
        return crc32.getValue();
    }
}
