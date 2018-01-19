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

import com.google.common.base.MoreObjects;
import io.jarasandha.util.misc.Validateable;
import lombok.Getter;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
public class FileBlockPosition implements Validateable<FileBlockPosition> {
    private final byte[] fileUuid;
    private final byte[] fileExtension;
    private final int blockPosition;
    /**
     * Cache to speed up lookups.
     */
    private int hashCode;

    /**
     * @param fileId
     * @param blockPosition
     * @see #toFileId()
     */
    public FileBlockPosition(FileId fileId, int blockPosition) {
        this.fileUuid = fileId.getUuid();
        this.fileExtension = fileId.getExtension();
        this.blockPosition = blockPosition;
    }

    @Override
    public FileBlockPosition validate() {
        checkNotNull(fileUuid, "fileUuid");
        checkNotNull(fileExtension, "fileExtension");
        checkArgument(blockPosition >= 0, "blockPosition");
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileBlockPosition that = (FileBlockPosition) o;
        if (blockPosition != that.blockPosition) {
            return false;
        }
        if (!Arrays.equals(fileUuid, that.fileUuid)) {
            return false;
        }
        return Arrays.equals(fileExtension, that.fileExtension);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int result = Arrays.hashCode(fileUuid);
            result = 31 * result + Arrays.hashCode(fileExtension);
            result = 31 * result + blockPosition;
            hashCode = result;
        }
        return hashCode;
    }

    public FileId toFileId() {
        return new FileId(fileUuid, fileExtension);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("fileId", toFileId())
                .add("blockPosition", blockPosition)
                .toString();
    }
}
