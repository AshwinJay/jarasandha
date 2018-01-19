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
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class DefaultFileEventListener implements FileEventListener {
    @Override
    public void onMiss(FileId fileId, File missingFile) {
        if (log.isDebugEnabled()) {
            log.debug("File id [{}] at [{}] appears to be missing", fileId, missingFile.getAbsolutePath());
        }
    }

    @Override
    public void onRemove(FileId fileId, File removableFile) {
        if (log.isDebugEnabled()) {
            log.debug("File id [{}] at [{}] can be safely deleted", fileId, removableFile.getAbsolutePath());
        }
    }
}
