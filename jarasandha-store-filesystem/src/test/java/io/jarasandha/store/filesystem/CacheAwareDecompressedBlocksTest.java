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
import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.Uuids;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.SynchronousQueue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by ashwin.jayaprakash.
 */
public class CacheAwareDecompressedBlocksTest {
    @Test
    public void testByteBufRefCount() throws InterruptedException {
        final SynchronousQueue<FileBlockPosition> blockRemovalQueue = new SynchronousQueue<>();
        final ReadersParameters parameters = new ReadersParameters()
                .maxTotalUncompressedBlockBytes(2048)
                .expireAfterAccess(Duration.ofDays(2))
                .metricRegistry(new MetricRegistry())
                .blockEventListener(blockRemovalQueue::offer);
        final CacheAwareDecompressedBlocks decompressedBlocks =
                new CacheAwareDecompressedBlocks(parameters, "debug-metrics");
        final FileId fileId = new FileId(Uuids.newUuid(), "test1");
        final FileBlockPosition position = new FileBlockPosition(fileId, 23);
        final BlockWithRecordOffsets mockBlock = mock(BlockWithRecordOffsets.class);

        //Empty.
        {
            DecompressedBlock block = decompressedBlocks.slice(position);
            //Obviously not present.
            assertNull(block);
        }

        //Empty, then save, then retrieve.
        {
            final byte[] bytes = "this is a test".getBytes(UTF_8);
            final ByteBufAllocator bufAllocator = new PooledByteBufAllocator(true);
            ByteBuf byteBuf = bufAllocator.buffer().writeBytes(bytes);
            DecompressedBlock block = new DecompressedBlock(mockBlock, byteBuf);
            //Put it explicitly.
            decompressedBlocks.save(position, block);
            assertEquals(1, byteBuf.refCnt());

            //Then retrieve it and verify.
            DecompressedBlock block2 = decompressedBlocks.slice(position);
            assertNotNull(block2);
            assertSame(mockBlock, block2.getBlock());
            ByteBuf byteBuf2 = block2.getDecompressedBlockBytes();
            //Different bufs.
            assertNotSame(byteBuf, byteBuf2);
            //But same content.
            assertEquals(0, ByteBufUtil.compare(byteBuf, byteBuf2));
            //And the ref count shows the existence of another ref.
            assertEquals(2, byteBuf.refCnt());
            //Ref count only shows itself.
            assertEquals(1, byteBuf2.refCnt());

            //Clear the cache and confirm the block removal.
            decompressedBlocks.invalidateAll();
            FileBlockPosition removedPosition = blockRemovalQueue.take();
            assertSame(position, removedPosition);
            //The removal has completed and the ref count should just be 1.
            assertEquals(1, byteBuf.refCnt());
            assertEquals(1, byteBuf2.refCnt());
            //Confirm nothing in the cache.
            assertNull(decompressedBlocks.slice(position));

            //Decrease manually.
            byteBuf2.release();
            assertEquals(0, byteBuf.refCnt());
            assertEquals(0, byteBuf2.refCnt());
        }

        decompressedBlocks.invalidateAll();
        decompressedBlocks.close();
    }
}
