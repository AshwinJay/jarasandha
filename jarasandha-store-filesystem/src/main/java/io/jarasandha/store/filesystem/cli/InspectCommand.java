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
package io.jarasandha.store.filesystem.cli;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Command(
        name = "inspect",
        header = "" +
                "Command line tool to inspect '" + Importer.OUTPUT_FILE_EXTENSION + "' files or files encoded by the" +
                " same process." +
                "\n"
)
@Getter
@Setter
@ToString(exclude = "printUsage")
public class InspectCommand implements Runnable {
    /**
     * Bug: https://github.com/remkop/picocli/issues/203
     */
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message")
    private boolean printUsage;

    @Option(names = {"-g", "--glob"}, description = "The input path name pattern to match (Ex: **.data)")
    private String inputGlob = "**.*";

    @Parameters(description = "The input files or directories")
    private File[] inputFilesOrDirs;

    @Override
    public void run() {
        if (printUsage) {
            CommandLine.usage(this, System.out);
        } else {
            checkArgument(inputFilesOrDirs != null && inputFilesOrDirs.length > 0,
                    "At least 1 input file or directory should be provided. The input glob is optional");

            final List<File> inputFileList = new LinkedList<>();
            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + inputGlob);

            for (File inputFile : inputFilesOrDirs) {
                try {
                    Files.walkFileTree(
                            inputFile.toPath(),
                            EnumSet.noneOf(FileVisitOption.class),
                            2,
                            new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                                    if (matcher.matches(path)) {
                                        inputFileList.add(path.toFile());
                                    }
                                    return FileVisitResult.CONTINUE;
                                }
                            }
                    );
                } catch (IOException e) {
                    System.out.println("Error occurred while processing files");
                    e.printStackTrace(System.out);
                }
            }

            new Inspector(inputFileList).run();
        }
    }
}
