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

import io.jarasandha.store.api.StoreWriters.Parameters;
import io.jarasandha.util.io.Persistable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Created by ashwin.jayaprakash.
 */
public interface StoreWriters<STORE_ID extends Persistable, PARAMS extends Parameters> extends AutoCloseable {
    StoreWriter<STORE_ID> newStoreWriter(PARAMS parameters);

    StoreWriter<STORE_ID> newStoreWriter(STORE_ID storeId, PARAMS parameters);

    @Getter
    @Setter
    @Accessors(chain = true)
    abstract class Parameters {
    }
}
