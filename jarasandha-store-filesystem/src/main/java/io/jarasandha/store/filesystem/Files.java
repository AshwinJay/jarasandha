/**
 * Copyright 2018 The Jarasandha.io project authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.store.filesystem;

import com.google.common.base.Throwables;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.CallerMustRelease;
import io.jarasandha.util.misc.Uuids;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.createDirectories;

/**
 * Utility class to:
 * <p>
 * Create unique files using {@link UUID} as the name, in a directory structure based on the segments in the
 * UUID. The depth of sub-directories is decided based on {@link #getUuidSplitLimit()}.
 * <p>
 * This helps to spread out the files over many directories instead of one single, large one.
 * <p>
 * It can also be used to navigate to a file using the {@link FileId}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
@ThreadSafe
public final class Files {
    private static final int UUID_SPLIT_LIMIT = 2;
    private static final String SPLIT_SEPARATOR = "-";
    @Getter
    private final File directory;
    @Getter
    private final int uuidSplitLimit;

    public Files(File directory) {
        this(directory, UUID_SPLIT_LIMIT);
    }

    public Files(File directory, int uuidSplitLimit) {
        checkArgument(directory.exists() && directory.isDirectory() && directory.canWrite(),
                "The path provided [%s] should be a directory that exists and has to be writable",
                directory.getAbsolutePath());
        checkArgument(uuidSplitLimit >= 1, "uuidSplitLimit should be a positive integer");

        this.directory = directory;
        this.uuidSplitLimit = uuidSplitLimit;
    }

    /**
     * Creates the path to the file by creating all the directories leading to the file but does not create the
     * file itself.
     *
     * @param extension See {@link FileId#FileId(UUID, String)}.
     * @return The actual {@link File} and its representative {@link FileId}.
     */
    public Pair<File, FileId> newFile(String extension) {
        for (; ; ) {
            UUID uuid = Uuids.newUuid();
            toFile(uuid, extension);
            try {
                return newFile(uuid, extension);
            } catch (FileAlreadyExistsException e) {
                log.info("Generated file name collides with an existing file. A new file name will be generated", e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates the path to the file by creating all the directories leading to the file but does not create the
     * file itself.
     *
     * @param uuid
     * @param extension
     * @return
     * @throws IOException
     * @throws FileAlreadyExistsException If a file or directory exists with that name.
     */
    public Pair<File, FileId> newFile(UUID uuid, String extension) throws IOException {
        File file = toFile(uuid, extension);
        log.debug("Attempting to create directories leading to file [{}]", file);

        Path filePath = file.toPath();
        Path parentPath = filePath.getParent().toAbsolutePath();
        createDirectories(parentPath);
        log.debug("Directory [{}] looks ok", parentPath.toAbsolutePath());
        if (file.exists()) {
            throw new FileAlreadyExistsException("[" + file.getAbsolutePath() + "] exists");
        }
        log.debug("File [{}] looks ok", filePath.toAbsolutePath());

        return Tuples.pair(file, new FileId(uuid, extension));
    }

    /**
     * {@link UUID} of this format:
     * {@code <time_low> "-" <time_mid> "-" <time_high_and_version> "-" <variant_and_sequence> "-" <node>}
     * becomes a file with this parent directory structure:
     * {@link #directory} {@code / <time_low> / <time_mid> / <time_high_and_version>-<variant_and_sequence> /
     * <node><extension>}
     *
     * @param uuid
     * @param extension
     * @return Existence of file is <b>not</b> verified.
     */
    public File toFile(UUID uuid, String extension) {
        String s = uuid.toString();
        String[] components = s.split(SPLIT_SEPARATOR, uuidSplitLimit);
        //Append the extension to the last component of the array.
        components[components.length - 1] = components[components.length - 1] + FileId.EXTENSION_SEPARATOR + extension;
        File file = directory;
        for (String component : components) {
            file = new File(file, component);
        }
        return file;
    }

    /**
     * @return {@link Pair}s of the {@link File} and the "UUID" of the file. These are all the files under the directory.
     */
    @CallerMustRelease
    public Stream<Pair<File, String>> showAll() {
        try {
            return java.nio.file.Files
                    .walk(directory.toPath())
                    .map(Path::toFile)
                    .map(File::getAbsoluteFile)
                    .filter(File::isFile)
                    .map(actualFile -> {
                        final LinkedList<String> nameStack = new LinkedList<>();
                        File file = actualFile;
                        int stringLength = 0;
                        for (int i = uuidSplitLimit; i > 0; i--) {
                            final String name = file.getName();

                            nameStack.addFirst(name);
                            stringLength += name.length();
                            if (i > 1) {
                                nameStack.addFirst(SPLIT_SEPARATOR);
                                stringLength += SPLIT_SEPARATOR.length();
                            }
                            file = file.getParentFile();
                        }

                        StringBuilder stringBuilder = new StringBuilder(stringLength);
                        nameStack.forEach(stringBuilder::append);
                        return Tuples.pair(actualFile, stringBuilder.toString());
                    });
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public File toFile(FileId fileId) {
        UUID uuid = fileId.toUUID();
        String extension = fileId.toExtension();
        return toFile(uuid, extension);
    }
}
