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
import io.netty.buffer.ByteBuf;

/**
 * Created by ashwin.jayaprakash.
 * <p>
 * The implementation of this interface is not expected to throw any exceptions.
 *
 * @param <STORE_ID>
 * @param <STORE_INFO>
 * @param <BLK_INFO>
 */
public interface StoreWriteProgressListener
        <STORE_ID extends Persistable, STORE_INFO extends StoreInfo<STORE_ID>, BLK_INFO extends StoreBlockInfo> {

    /**
     * @param storeInfo
     * @return True if the {@link #recordWritten(LogicalRecordLocation, ByteBuf)} must be called with a copy of the
     * original record. If false, then null will be sent instead.
     */
    default boolean storeStarted(STORE_INFO storeInfo) {
        return false;
    }

    /**
     * Default implementation {@link ByteBuf#release() releases} the record.
     *
     * @param recordLocation
     * @param recordCopyOrNull Null if {@link #storeStarted(StoreInfo)} returned false. Otherwise a copy of the record,
     *                         which must be {@link ByteBuf#release() released} by the implementation of this method.
     */
    default void recordWritten(LogicalRecordLocation recordLocation, ByteBuf recordCopyOrNull) {
        if (recordCopyOrNull != null) {
            recordCopyOrNull.release();
        }
    }

    default void blockCompleted(BLK_INFO blockInfo) {
    }

    default void storeClosed() {
    }

    default void storeFailed(StoreException exception) {
    }
}
