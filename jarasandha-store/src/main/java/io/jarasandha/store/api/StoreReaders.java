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
package io.jarasandha.store.api;

import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.util.collection.ImmutableBitSet;
import io.jarasandha.util.io.Persistable;
import io.netty.buffer.ByteBuf;

/**
 * Created by ashwin.jayaprakash.
 *
 * @param <STORE_ID>
 */
public interface StoreReaders<STORE_ID extends Persistable> extends AutoCloseable {
    /**
     * Read the outline of the store.
     *
     * @param storeId
     * @param blocksToRead
     * @return
     * @throws StoreException
     */
    Index<? extends BlockWithRecordOffsets> readIndex(STORE_ID storeId, ImmutableBitSet blocksToRead);

    /**
     * Read the entire uncompressed block.
     *
     * @param storeId
     * @param positionOfBlock
     * @return Uncompressed block.
     * @throws StoreException
     */
    ByteBuf readBlock(STORE_ID storeId, int positionOfBlock);

    /**
     * Read a specific record.
     *
     * @param storeId
     * @param logical
     * @return
     * @throws StoreReadException
     */
    ByteBuf readRecord(STORE_ID storeId, LogicalRecordLocation logical);

    /**
     * Release any open resources related to the store.
     *
     * @param storeId
     * @throws StoreException
     */
    void release(STORE_ID storeId);
}
