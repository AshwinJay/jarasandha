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
import io.jarasandha.util.test.AbstractTest;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class FilesTest extends AbstractTest {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void test_FileCreation_AndFileInfoRoundTrip() throws Exception {
        final Files files = new Files(tempFolder.newFolder(), 2);
        final File directory = files.getDirectory();
        final String extension = ".x";

        Pair<File, FileId> fileInfo = files.newFile(extension);
        File file = fileInfo.getOne();
        assertTrue(file.createNewFile());
        log.info("Created [{}] within [{}]", file.getAbsolutePath(), directory.getAbsolutePath());
        //A new empty file is created automatically including parent dirs.
        assertTrue(file.exists());
        assertEquals(0, file.length());
        assertTrue(file.getName().endsWith(extension));

        File parentFile = file.getParentFile();
        assertTrue(parentFile.exists());
        assertTrue(parentFile.isDirectory());
        //2 levels.
        assertEquals(directory, parentFile.getParentFile());

        //Round trip.
        File sameFile = files.toFile(fileInfo.getTwo());
        assertEquals(file, sameFile);
    }
}
