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

import io.jarasandha.store.api.StoreWriters.Parameters;
import io.jarasandha.util.misc.ByteSize.Unit;
import io.jarasandha.util.misc.Validateable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Ints.checkedCast;
import static io.jarasandha.util.misc.ByteSize.Unit.MEGABYTES;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@Setter
@Accessors(fluent = true)
@ToString
public class WriterParameters extends Parameters implements Validateable<WriterParameters> {
    public static final int SIZE_CHUNK_BYTES = checkedCast(Unit.KILOBYTES.toBytes() * 16);
    private long fileSizeBytesLimit = MEGABYTES.toBytes() * 256;
    private int uncompressedBytesPerBlockLimit = checkedCast(Unit.MEGABYTES.toBytes() * 16);
    private boolean blocksCompressed = true;
    private boolean indexCompressed = true;
    private int writeChunkSizeBytes = SIZE_CHUNK_BYTES;

    @Override
    public WriterParameters validate() {
        checkArgument(fileSizeBytesLimit > 0, "fileSizeBytesLimit");
        checkArgument(uncompressedBytesPerBlockLimit > 0, "uncompressedBytesPerBlockLimit");
        checkArgument(uncompressedBytesPerBlockLimit < fileSizeBytesLimit, "uncompressedBytesPerBlockLimit");
        checkArgument(writeChunkSizeBytes > 0, "writeChunkSizeBytes");

        return this;
    }
}
