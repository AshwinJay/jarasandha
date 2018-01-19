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

import io.jarasandha.util.io.Persistable;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBuf;

/**
 * Created by ashwin.jayaprakash.
 */
public interface StoreWriter<STORE_ID extends Persistable> extends AutoCloseable {
    STORE_ID getStoreId();

    boolean isBlocksCompressed();

    boolean isIndexCompressed();

    /**
     * Writes the given record if there is room.
     * <p>
     * Records are written in blocks.
     * <p>
     * Records may not be immediately flushed. It could be done:
     * <p>
     * - On {@link #close()} (if required)
     * <p>
     * - Or if the current block does not have sufficient room to accommodate the current record.
     * <p>
     * If block compression is enabled ({@link #isBlocksCompressed()}), then the block is first compressed and then the
     * compressed block is written.
     *
     * @param value
     * @return
     * @throws StoreFullException    When the store cannot accommodate any further writes. This means, the next step is
     *                               to call {@link #close()}.
     * @throws RecordTooBigException When the record does not fit within the limits specified.
     * @throws StoreException        All other cases and an indication that something went wrong.
     */
    LogicalRecordLocation write(@CallerMustRelease ByteBuf value);

    @Override
    void close();
}
