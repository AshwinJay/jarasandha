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

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.misc.Uuids;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ashwin.jayaprakash.
 */
public class CacheAwareBlockReaderTest {
    private final int numBlocks = 96;
    private final int mockNumRecordsInBlock = 888;
    private BlockWithRecordOffsets mockBlock;
    private Cache<FileBlockPosition, BlockWithRecordOffsets> blockCache;
    private CacheAwareBlockReader blockReader;

    @Before
    public void setUp() {
        mockBlock = mock(BlockWithRecordOffsets.class);
        when(mockBlock.numRecords()).thenReturn(-1);

        blockCache = Caffeine.newBuilder().build();

        blockReader = new CacheAwareBlockReader(
                blockCache, new MetricRegistry(), "debug-reader"
        ) {
            @Override
            Index<? extends BlockWithRecordOffsets> readUncachedIndex(
                    ByteBufAllocator allocator, FileChannel fileChannel, Tail tail,
                    FileId fileId, ImmutableBitSet blocksToRead
            ) {
                return new Index<BlockWithRecordOffsets>() {
                    @Override
                    public List<? extends BlockWithRecordOffsets> blocks() {
                        LinkedList<BlockWithRecordOffsets> list = new LinkedList<>();
                        for (int i = 0; i < numBlocks; i++) {
                            final int j = blocksToRead.nextSetBit(i);
                            BlockWithRecordOffsets block = mockBlock;
                            if (i == j) {
                                block = mock(BlockWithRecordOffsets.class);
                                when(block.numRecords()).thenReturn(mockNumRecordsInBlock);
                            }
                            list.add(block);
                        }
                        return list;
                    }
                };
            }
        };
    }

    @Test
    public void test_readIndex_selectiveBlocks() {
        //Cache is empty.
        assertEquals(0, blockCache.asMap().size());
        Index<? extends BlockWithRecordOffsets> index = blockReader.read(
                new UnpooledByteBufAllocator(false), mock(FileChannel.class),
                mock(Tail.class), mock(FileId.class),
                /*Request these 3 blocks*/
                ImmutableBitSet.newBitSet(0, 9, numBlocks - 1)
        );
        //Cache has 3 blocks.
        assertEquals(3, blockCache.asMap().size());
        //Index also has all blocks but most of them are mocks.
        assertEquals(numBlocks, index.blocks().size());
        //Now check for the actual blocks that we requested.
        BlockWithRecordOffsets requestedBlock = index.blocks().get(0);
        assertEquals(mockNumRecordsInBlock, requestedBlock.numRecords());
        requestedBlock = index.blocks().get(9);
        assertEquals(mockNumRecordsInBlock, requestedBlock.numRecords());
        requestedBlock = index.blocks().get(numBlocks - 1);
        assertEquals(mockNumRecordsInBlock, requestedBlock.numRecords());
        //This one was not requested.
        BlockWithRecordOffsets notRequestedBlock = index.blocks().get(1);
        assertSame(mockBlock, notRequestedBlock);
    }

    @Test
    public void test_readIndex_noBlocks() {
        //Cache is empty.
        assertEquals(0, blockCache.asMap().size());
        Index<? extends BlockWithRecordOffsets> index = blockReader.read(
                new UnpooledByteBufAllocator(false), mock(FileChannel.class),
                mock(Tail.class), mock(FileId.class),
                /*Request none*/
                ImmutableBitSet.newBitSet()
        );
        //Cache still empty.
        assertEquals(0, blockCache.asMap().size());
        //Index also has all blocks but all of them are mocks.
        assertEquals(numBlocks, index.blocks().size());
        //None was requested.
        for (BlockWithRecordOffsets block : index.blocks()) {
            assertSame(mockBlock, block);
        }
    }

    @Test
    public void test_readBlock() {
        //Cache is empty.
        assertEquals(0, blockCache.asMap().size());
        final BlockWithRecordOffsets requestedBlock = blockReader.read(
                new UnpooledByteBufAllocator(false), mock(FileChannel.class),
                mock(Tail.class),
                new FileBlockPosition(new FileId(Uuids.newUuid(), "demo"), 24)
        );
        //Cache has 1 block.
        assertEquals(1, blockCache.asMap().size());
        assertEquals(mockNumRecordsInBlock, requestedBlock.numRecords());
        assertNotSame(mockBlock, requestedBlock);

        blockReader.invalidateAll();
        blockReader.close();
    }
}
