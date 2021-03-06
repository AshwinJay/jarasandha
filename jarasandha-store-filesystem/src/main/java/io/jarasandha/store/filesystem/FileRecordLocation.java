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

import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@EqualsAndHashCode
class FileRecordLocation {
    private final int recordStartByteOffsetInBlock;
    private final int recordSizeBytes;

    FileRecordLocation(int recordStartByteOffsetInBlock, int recordSizeBytes) {
        this.recordStartByteOffsetInBlock = recordStartByteOffsetInBlock;
        this.recordSizeBytes = recordSizeBytes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
                .add("recordStartByteOffsetInBlock", recordStartByteOffsetInBlock)
                .add("recordSizeBytes", recordSizeBytes)
                .toString();
    }
}
