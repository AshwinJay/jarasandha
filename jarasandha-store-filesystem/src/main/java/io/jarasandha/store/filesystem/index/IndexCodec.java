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
package io.jarasandha.store.filesystem.index;

import io.jarasandha.store.api.Block;
import io.jarasandha.store.api.BlockWithRecordSizes;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.api.StoreException.StoreCorruptException;
import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.collection.MutableIntsWrapper;
import io.jarasandha.util.collection.SparseFixedSizeList;
import io.jarasandha.util.io.ByteBufs;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.jarasandha.store.api.StoreException.checkCorruption;
import static io.jarasandha.util.io.ByteBufs.*;
import static io.jarasandha.util.io.Codecs.base32Ascii;
import static io.jarasandha.util.misc.Formatters.format;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
@Slf4j
public abstract class IndexCodec {
    private static final ByteBuf INDEX_MARKER = unreleasableBuffer(wrappedBuffer(
            ("¤§" + base32Ascii("INDEX") + "§¤").getBytes(US_ASCII)
    ).asReadOnly());
    private static final short INDEX_VERSION = 1;
    /**
     * Size of the encoded record size.
     */
    private static final int SIZE_OF_MESSAGE_SIZE_BYTES = SIZE_BYTES_INT;
    static final int SPARSE_LIST_MIN_SIZE = 128;

    private IndexCodec() {
    }

    /**
     * @param numBlocks
     * @param numRecordsPerBlock
     * @return The decompressed size estimate.
     */
    public static int estimateSizeOfIndexBytes(int numBlocks, int numRecordsPerBlock) {
        checkArgument((numBlocks > 0 && numRecordsPerBlock >= 0) || (numBlocks == 0 && numRecordsPerBlock == 0));

        return INDEX_MARKER.readableBytes() + /*Version*/ SIZE_BYTES_SHORT +

                numBlocks * ( /*Index block start offset*/ SIZE_BYTES_INT) +
                /*Index last block end offset*/
                SIZE_BYTES_INT +

                numBlocks * (
                        /*Block start position.*/ SIZE_BYTES_LONG +
                        /*Block end position.*/ SIZE_BYTES_LONG +
                        /*Num records in block.*/ SIZE_BYTES_INT +
                        /*Record sizes in block in bytes.*/ (SIZE_OF_MESSAGE_SIZE_BYTES * numRecordsPerBlock)
                );
    }

    /**
     * @param blocks
     * @return The decompressed size estimate.
     */
    @CostlyOperation
    public static int estimateSizeOfIndexBytes(List<? extends BlockWithRecordSizes> blocks) {
        return INDEX_MARKER.readableBytes() +  /*Version*/ SIZE_BYTES_SHORT +

                blocks.size() * ( /*Index block start offset*/ SIZE_BYTES_INT) +
                /*Index last block end offset*/
                SIZE_BYTES_INT +

                blocks
                        .stream()
                        .mapToInt(block -> {
                            return
                                    /*Block start position.*/ SIZE_BYTES_LONG +
                                    /*Block end position.*/ SIZE_BYTES_LONG +
                                    /*Num records in block.*/ SIZE_BYTES_INT +
                                    /*Record sizes in block in bytes.*/
                                    (SIZE_OF_MESSAGE_SIZE_BYTES * block.recordByteSizes().size());
                        })
                        .sum();
    }

    public static ByteBuf encode(
            ByteBufAllocator allocator, List<? extends BlockWithRecordSizes> blocks, boolean compress) {

        final ByteBuf byteBuf = allocator.buffer(4096);
        try {
            //Head.
            {
                byteBuf.writeBytes(INDEX_MARKER.slice());
                byteBuf.writeShort(INDEX_VERSION);
            }

            //Body.
            {
                //Set 0 for each block. We will come back to this and set the relative location of the index block.
                final int numBlocks = blocks.size();
                final int startOfIndexOffsets = byteBuf.writerIndex();
                for (int i = 0; i < numBlocks; i++) {
                    //Block start offset. These are relative offsets. Hence int and not long.
                    byteBuf.writeInt(0);
                }
                //Last block end offset.
                byteBuf.writeInt(0);

                final MutableIntList startAndEndOffsetsOfBlocks = new IntArrayList(numBlocks + /*Last end offset*/1);

                //Block details.
                int i = 0;
                for (BlockWithRecordSizes block : blocks) {
                    ByteBuf blockByteBuf = allocator.buffer(192);
                    try {
                        blockByteBuf.writeLong(block.blockStartByteOffset());
                        blockByteBuf.writeLong(block.blockEndByteOffset());
                        //Num of records in block.
                        blockByteBuf.writeInt(block.recordByteSizes().size());
                        block
                                .recordByteSizes()
                                .forEach(blockByteBuf::writeInt);

                        if (compress) {
                            final ByteBuf tempBlockByteBuf = ByteBufs.compress(allocator, blockByteBuf);
                            try {
                                blockByteBuf.release();
                            } catch (Throwable t) {
                                tempBlockByteBuf.release();
                                throw t;
                            }
                            blockByteBuf = tempBlockByteBuf;
                        }

                        //Record start offset.
                        startAndEndOffsetsOfBlocks.add(byteBuf.writerIndex());
                        //Then write the possibly compressed block details.
                        byteBuf.writeBytes(blockByteBuf);
                        if (i == numBlocks - 1) {
                            //Record the last block's end offset.
                            startAndEndOffsetsOfBlocks.add(byteBuf.writerIndex());
                        }
                        i++;
                    } finally {
                        blockByteBuf.release();
                    }
                }

                //Go back and write the index block start and end offsets.
                startAndEndOffsetsOfBlocks.forEachWithIndex((offset, index) -> {
                    byteBuf.setInt(startOfIndexOffsets + (index * SIZE_BYTES_INT), offset);
                });
            }

            if (log.isDebugEnabled()) {
                log.debug("Index size on disk is [{}] bytes with [{}] blocks",
                        format(byteBuf.readableBytes()), blocks.size());
            }
        } catch (Throwable t) {
            byteBuf.release();
            throw t;
        }
        return byteBuf;
    }

    /**
     * @param allocator
     * @param fileChannel
     * @param indexCompressed
     * @param indexStartOffset
     * @param indexEndOffset
     * @param numBlocks
     * @param blockFactory
     * @param blockNumbersToDecode
     * @param <B>
     * @return
     * @throws StoreCorruptException
     * @throws StoreReadException
     * @throws IllegalArgumentException
     */
    public static <B extends Block> Index<B> decode(
            ByteBufAllocator allocator, @CallerMustRelease FileChannel fileChannel,
            boolean indexCompressed, long indexStartOffset, long indexEndOffset, int numBlocks,
            BlockFactory<B> blockFactory, ImmutableBitSet blockNumbersToDecode
    ) {
        try {
            return mapAndApply(
                    fileChannel, indexStartOffset, indexEndOffset - indexStartOffset,
                    decoder(allocator, indexCompressed, numBlocks, blockFactory, blockNumbersToDecode)
            );
        } catch (IOException e) {
            throw new StoreReadException("Error occurred while reading the index", e);
        }
    }

    private static <B extends Block> Function<ByteBuf, Index<B>> decoder(
            ByteBufAllocator allocator, boolean indexCompressed, int numBlocks,
            BlockFactory<B> blockFactory, ImmutableBitSet blockNumbersToDecode
    ) {
        return allIndexBlocksByteBuf -> {
            //Check header.
            {
                final ByteBuf header = allocator.buffer(INDEX_MARKER.readableBytes());
                try {
                    allIndexBlocksByteBuf.readBytes(header);
                    checkCorruption(ByteBufUtil.compare(header, INDEX_MARKER) == 0,
                            "The index header does not match the expected header");
                } finally {
                    header.release();
                }

                final short version = allIndexBlocksByteBuf.readShort();
                checkCorruption(version == INDEX_VERSION,
                        "The index header version number [%d] from the file does not match the expected version [%d]",
                        version, INDEX_VERSION);
            }

            //Bookmark.
            final int startOfIndexOffsets = allIndexBlocksByteBuf.readerIndex();

            //Read the positions of the index blocks that we are supposed to readIndex.
            final MutableIntObjectMap<IntIntPair> indexBlockStartEndOffsets = new IntObjectHashMap<>();
            for (int blockNum = 0; blockNum < numBlocks; ) {
                final int blockToDecode = blockNumbersToDecode.nextSetBit(blockNum);
                if (blockToDecode == ImmutableBitSet.POSITION_NOT_SET) {
                    //No more positions needed to decode.
                    break;
                }
                //Rewind.
                allIndexBlocksByteBuf.readerIndex(startOfIndexOffsets + (blockToDecode * SIZE_BYTES_INT));
                int startOffset = allIndexBlocksByteBuf.readInt();
                //This is actually the start offset of the next block or the special, last end offset.
                int endOffset = allIndexBlocksByteBuf.readInt();
                indexBlockStartEndOffsets.put(blockToDecode, PrimitiveTuples.pair(startOffset, endOffset));
                //Move next.
                blockNum = blockToDecode + 1;
            }

            //Read the index blocks that we are supposed to readIndex.
            final B skippedBlock = blockFactory.makeSkipped();
            final List<B> blocks;
            final boolean sparseBlocks;
            //Use this if a large number of blocks were skipped.
            if (numBlocks > SPARSE_LIST_MIN_SIZE && (indexBlockStartEndOffsets.size() / numBlocks) < 0.5) {
                blocks = new SparseFixedSizeList<>(numBlocks, skippedBlock);
                sparseBlocks = true;
            } else {
                blocks = new FastList<>(numBlocks);
                sparseBlocks = false;
            }

            for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
                final B block;
                if (blockNumbersToDecode.nextSetBit(blockNum) == blockNum) {
                    //We do this because we need to iterate over the map in order. Otherwise we will
                    //end up swinging back and forth on disk.
                    final IntIntPair offsetRange = indexBlockStartEndOffsets.get(blockNum);

                    //Move the offset to the beginning of the index block.
                    allIndexBlocksByteBuf.readerIndex(offsetRange.getOne());
                    ByteBuf indexBlockByteBuf =
                            allocator.buffer(offsetRange.getTwo() - offsetRange.getOne());
                    try {
                        //Read the index block.
                        allIndexBlocksByteBuf.readBytes(indexBlockByteBuf);
                        if (indexCompressed) {
                            final ByteBuf decompressedByteBuf;
                            try {
                                decompressedByteBuf = decompress(allocator, indexBlockByteBuf);
                            } finally {
                                indexBlockByteBuf.release();
                            }
                            indexBlockByteBuf = decompressedByteBuf;
                        }

                        block = decodeIndexBlock(blockFactory, indexBlockByteBuf);
                    } finally {
                        indexBlockByteBuf.release();
                    }
                } else {
                    block = skippedBlock;
                }

                if (!sparseBlocks) {
                    //Non-sparse blocks does not support set(position, block) at the end.
                    blocks.add(block);
                } else if (block != skippedBlock) {
                    //No need to set in case of sparse blocks.
                    blocks.set(blockNum, block);
                }
            }

            return Builders.buildIndex(blocks);
        };
    }

    private static <B extends Block> B decodeIndexBlock(BlockFactory<B> blockFactory, ByteBuf indexBlockBytes) {
        final long blockStartByteOffset = indexBlockBytes.readLong();
        final long blockEndByteOffset = indexBlockBytes.readLong();
        final int numRecordsInBlock = indexBlockBytes.readInt();
        final MutableIntList recordByteSizes = new IntArrayList(numRecordsInBlock);
        for (int i = 0; i < numRecordsInBlock; i++) {
            recordByteSizes.add(indexBlockBytes.readInt());
        }
        return blockFactory.make(blockStartByteOffset, blockEndByteOffset, new MutableIntsWrapper(recordByteSizes));
    }
}
