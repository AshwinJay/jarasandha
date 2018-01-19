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
package io.jarasandha.store.filesystem.shared;

import io.jarasandha.util.io.Persistable;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.CostlyOperation;
import io.jarasandha.util.misc.Uuids;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.Getter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
public class FileId implements Persistable {
    public static final String EXTENSION_SEPARATOR = ".";
    private final byte[] uuid;
    private final byte[] extension;
    /**
     * Cache to speed up look ups.
     */
    private int hashCode;

    /**
     * @param uuid
     * @param extension Stored in {@link StandardCharsets#UTF_8} form. May be empty but not null. This should
     *                  <b>not</b> have the starting "{@value #EXTENSION_SEPARATOR}"
     */
    public FileId(UUID uuid, String extension) {
        this(Uuids.bytesFromUuid(checkNotNull(uuid)), checkNotNull(extension).getBytes(UTF_8));
    }

    /**
     * @param uuid      Not the {@link StandardCharsets#UTF_8} form but {@link Uuids#bytesFromUuid(UUID)}.
     * @param extension See {@link #FileId(UUID, String)}.
     */
    public FileId(byte[] uuid, byte[] extension) {
        this.uuid = checkNotNull(uuid);
        this.extension = checkNotNull(extension);
    }

    /**
     * @param fileIdBytes
     * @see #toBytes(ByteBufAllocator)
     */
    public FileId from(@CallerMustRelease ByteBuf fileIdBytes) {
        checkArgument(fileIdBytes.readableBytes() >= Uuids.UUID_BYTES_SIZE);

        byte[] uuid = new byte[Uuids.UUID_BYTES_SIZE];
        fileIdBytes.readBytes(this.uuid);

        int extensionLength = fileIdBytes.readableBytes();
        byte[] extension = new byte[extensionLength];
        fileIdBytes.readBytes(this.extension);

        return new FileId(uuid, extension);
    }

    public static FileId from(File file) {
        final String fileNameAndExt = file.getName();
        final int extensionPos = fileNameAndExt.indexOf(EXTENSION_SEPARATOR);
        final String errExtMessage =
                "File name should have a name and extension part separated by '" + EXTENSION_SEPARATOR + "'";
        checkArgument(extensionPos > -1 && extensionPos < fileNameAndExt.length() - 1, errExtMessage);

        final String fileName = fileNameAndExt.substring(0, extensionPos);
        checkArgument(fileName.length() > 0, "File name is should have non-zero length");
        final UUID uuid = UUID.nameUUIDFromBytes(fileName.getBytes(UTF_8));

        final String extension = fileNameAndExt.substring(extensionPos + 1);
        checkArgument(extension.length() > 0, "File extension is should have non-zero length");

        return new FileId(uuid, extension);
    }

    /**
     * @return Just the {@link #getUuid()} part.
     */
    public UUID toUUID() {
        return Uuids.uuidFromBytes(uuid);
    }

    /**
     * @return {@link StandardCharsets#UTF_8} form of {@link #getExtension()}.
     */
    public String toExtension() {
        return new String(extension, UTF_8);
    }

    @CostlyOperation
    @Override
    public String toString() {
        return toUUID().toString() + EXTENSION_SEPARATOR + toExtension();
    }

    /**
     * Not the same as {@link StandardCharsets#UTF_8} {@link #toString() string}.
     *
     * @param allocator
     * @return
     */
    @Override
    public ByteBuf toBytes(ByteBufAllocator allocator) {
        ByteBuf byteBuf = allocator.buffer(uuid.length + extension.length);
        byteBuf.writeBytes(uuid);
        byteBuf.writeBytes(extension);
        return byteBuf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileId)) {
            return false;
        }
        FileId fileId = (FileId) o;
        return Arrays.equals(uuid, fileId.uuid) &&
                Arrays.equals(extension, fileId.extension);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int h = Arrays.hashCode(uuid);
            h = 31 * h + Arrays.hashCode(extension);
            hashCode = h;
        }
        return hashCode;
    }
}
