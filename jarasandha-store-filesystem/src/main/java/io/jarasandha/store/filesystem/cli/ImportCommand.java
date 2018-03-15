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

import com.google.common.collect.Lists;
import io.jarasandha.store.filesystem.WriterParameters;
import io.jarasandha.util.misc.ByteSize.Unit;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.primitives.Ints.checkedCast;
import static io.jarasandha.util.misc.ByteSize.Unit.MEGABYTES;

@Command(
        name = "import",
        header = "" +
                "Command line tool to read and import records in a file separated by newline." +
                "Multiple input files can be provided. The output files are written to disk with " +
                "the '" + Importer.OUTPUT_FILE_EXTENSION + "' extension." +
                "\n"
)
@Getter
@Setter
@ToString(exclude = "printUsage")
public class ImportCommand implements Runnable {
    /**
     * Bug: https://github.com/remkop/picocli/issues/203
     */
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message")
    private boolean printUsage;

    @Option(names = {"-o", "--out-dir"}, required = true, description = "The output directory")
    private File outputDir;

    @Parameters(description = "The input files. If not provided, then in-memory records will be used")
    private File[] inputFiles;

    @Option(names = {"--file-size"}, description = "The maximum size in bytes of the output files")
    private long fileSizeBytesLimit = MEGABYTES.toBytes() * 256;

    @Option(names = {"--block-size"}, description = "The maximum (uncompressed) size in bytes of the file blocks")
    private int uncompressedBytesPerBlockLimit = checkedCast(Unit.MEGABYTES.toBytes() * 16);

    @Option(names = {"--no-block-comp"}, description = "Compression the blocks before writing to the file")
    private boolean blocksCompressed = true;

    @Option(names = {"--no-index-comp"}, description = "Compression the index before writing to the file")
    private boolean indexCompressed = true;

    @Option(names = {"--chunk-size"}, description = "The granularity in bytes of file write operations")
    private int writeChunkSizeBytes = checkedCast(Unit.KILOBYTES.toBytes() * 16);

    @Override
    public void run() {
        if (printUsage) {
            CommandLine.usage(this, System.out);
        } else {
            WriterParameters writerParameters = new WriterParameters()
                    .fileSizeBytesLimit(fileSizeBytesLimit)
                    .uncompressedBytesPerBlockLimit(uncompressedBytesPerBlockLimit)
                    .blocksCompressed(blocksCompressed)
                    .indexCompressed(indexCompressed)
                    .writeChunkSizeBytes(writeChunkSizeBytes);
            List<File> inputFileList = inputFiles == null ? new ArrayList<>() : Lists.newArrayList(inputFiles);

            new Importer(inputFileList, outputDir, writerParameters).run();
        }
    }
}
