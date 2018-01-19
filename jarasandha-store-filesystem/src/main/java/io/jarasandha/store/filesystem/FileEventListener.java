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

import io.jarasandha.store.filesystem.shared.FileId;
import net.jcip.annotations.ThreadSafe;

import java.io.File;

/**
 * Hooks to handle certain scenarios.
 */
@ThreadSafe
public interface FileEventListener {
    default void start() {

    }

    /**
     * A typical implementation would be to fetch the file from an archive and place it at the location where it is
     * expected.
     *
     * @param fileId
     * @param missingFile The file that has been requested but was missing in the in-memory cache (may still be on
     *                    disk).
     */
    default void onMiss(FileId fileId, File missingFile) {

    }

    /**
     * @param fileId
     * @param removableFile The file that is no longer needed (until it is requested for again through
     *                      {@link #onMiss(FileId, File)}.
     */
    default void onRemove(FileId fileId, File removableFile) {

    }

    default void stop() {

    }
}
