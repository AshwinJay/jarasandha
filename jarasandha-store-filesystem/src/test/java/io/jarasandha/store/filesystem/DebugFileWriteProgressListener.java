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

import io.jarasandha.store.api.StoreException;
import io.jarasandha.store.filesystem.shared.FileInfo;

import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
class DebugFileWriteProgressListener extends FileWriteProgressListener {
    DebugFileWriteProgressListener() {
    }

    @Override
    public boolean storeStarted(FileInfo fileInfo) {
        super.storeStarted(fileInfo);
        assertFileInfoValid();
        return true;
    }

    @Override
    public void storeClosed() {
        super.storeClosed();
        assertFileInfoValid();
    }

    @Override
    public void storeFailed(StoreException exception) {
        super.storeFailed(exception);
        assertFileInfoValid();
    }

    private void assertFileInfoValid() {
        assertTrue(fileInfo().file().exists());
        assertTrue(fileInfo().fileChannel().isOpen());
    }
}
