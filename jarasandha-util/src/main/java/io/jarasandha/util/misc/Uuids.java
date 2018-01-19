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
package io.jarasandha.util.misc;

import com.fasterxml.uuid.impl.UUIDUtil;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Created by ashwin.jayaprakash.
 */
public abstract class Uuids {
    public static final int UUID_BYTES_SIZE = 16;

    private Uuids() {
    }

    public static UUID newUuid() {
        return UUID.randomUUID();
    }

    public static ByteBuf newUuidByteBuf() {
        return wrappedBuffer(bytesFromUuid(newUuid()));
    }

    public static byte[] bytesFromUuid(UUID uuid) {
        return UUIDUtil.asByteArray(uuid);
    }

    /**
     * @param uuidBytes From {@link #bytesFromUuid(UUID)}.
     * @return
     */
    public static UUID uuidFromBytes(byte[] uuidBytes) {
        return UUIDUtil.uuid(uuidBytes);
    }
}
