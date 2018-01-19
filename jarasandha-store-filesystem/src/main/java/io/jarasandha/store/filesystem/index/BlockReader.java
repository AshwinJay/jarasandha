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

import io.jarasandha.store.api.BlockWithRecordOffsets;
import io.jarasandha.store.api.Index;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.tail.Tail;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBufAllocator;
import net.jcip.annotations.ThreadSafe;

import java.nio.channels.FileChannel;

import static io.jarasandha.store.filesystem.index.IndexCodec.decode;

/**
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
public interface BlockReader {
    default BlockWithRecordOffsets read(
            ByteBufAllocator allocator, @CallerMustRelease FileChannel fileChannel,
            Tail tail, FileBlockPosition blockPosition
    ) {
        final ImmutableBitSet blockPositionsToDecode = ImmutableBitSet.newBitSet(blockPosition.getBlockPosition());
        final Index<? extends BlockWithRecordOffsets> index =
                read(allocator, fileChannel, tail, blockPosition.toFileId(), blockPositionsToDecode);
        //Return only the block that was asked for.
        return index.blocks().get(blockPosition.getBlockPosition());
    }

    /**
     * Read only selective blocks of the {@link Index}.
     *
     * @param allocator
     * @param fileChannel
     * @param tail
     * @param fileId
     * @param blocksToRead
     * @return
     */
    default Index<? extends BlockWithRecordOffsets> read(
            ByteBufAllocator allocator, @CallerMustRelease FileChannel fileChannel, Tail tail, FileId fileId,
            ImmutableBitSet blocksToRead
    ) {
        return readFromFile(allocator, fileChannel, tail, blocksToRead);
    }

    static Index<? extends BlockWithRecordOffsets> readFromFile(
            ByteBufAllocator allocator,
            @CallerMustRelease FileChannel fileChannel, Tail tail,
            ImmutableBitSet blockPositionsToRead
    ) {
        return decode(
                allocator, fileChannel,
                tail.isIndexCompressed(), tail.getIndexStartOffset(), tail.getIndexEndOffset(),
                tail.getNumBlocks(), Builders::buildBlockWithRecordOffsets, blockPositionsToRead
        );
    }
}
