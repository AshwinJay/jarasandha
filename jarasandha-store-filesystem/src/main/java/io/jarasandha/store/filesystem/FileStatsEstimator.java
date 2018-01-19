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

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.jarasandha.store.filesystem.index.IndexCodec.estimateSizeOfIndexBytes;
import static io.jarasandha.util.io.ByteBufs.SIZE_BYTES_CHECKSUM;
import static java.lang.Math.ceil;
import static java.lang.Math.floor;

/**
 * Created by ashwin.jayaprakash.
 */
@ToString
@Getter
@Accessors(fluent = true)
public class FileStatsEstimator {
    private final long uncompressedSizeOfDataInFileBytes;
    private final int uncompressedBlockSizeLimitBytes;
    private final int avgUncompressedRecordSizeBytes;
    private final int estimatedNumBlocks;
    private final int estimatedNumRecordsPerBlock;
    private final long estimatedSizeOfCompleteFileBytes;

    private FileStatsEstimator(long maxUncompressedDataPartOfFileBytes,
                               int uncompressedBlockSizeLimitBytes,
                               int avgUncompressedRecordSizeBytes) {

        checkArgument(avgUncompressedRecordSizeBytes > 0, "avgUncompressedRecordSizeBytes should be greater than 0");
        checkArgument(
                uncompressedBlockSizeLimitBytes > avgUncompressedRecordSizeBytes + SIZE_BYTES_CHECKSUM,
                "uncompressedBlockSizeLimitBytes should be greater than size of" +
                        " avgUncompressedRecordSizeBytes + " + SIZE_BYTES_CHECKSUM);
        checkArgument(maxUncompressedDataPartOfFileBytes > uncompressedBlockSizeLimitBytes,
                "maxUncompressedDataPartOfFileBytes should be greater than uncompressedBlockSizeLimitBytes");

        this.uncompressedSizeOfDataInFileBytes = maxUncompressedDataPartOfFileBytes;
        this.uncompressedBlockSizeLimitBytes = uncompressedBlockSizeLimitBytes;
        this.avgUncompressedRecordSizeBytes = avgUncompressedRecordSizeBytes;

        this.estimatedNumBlocks = (int) ceil(
                maxUncompressedDataPartOfFileBytes / (double) uncompressedBlockSizeLimitBytes
        );
        this.estimatedNumRecordsPerBlock = (int) floor(
                (uncompressedBlockSizeLimitBytes - SIZE_BYTES_CHECKSUM) /
                        (double) avgUncompressedRecordSizeBytes
        );
        this.estimatedSizeOfCompleteFileBytes =
                maxUncompressedDataPartOfFileBytes +
                        estimateSizeOfIndexBytes(estimatedNumBlocks, estimatedNumRecordsPerBlock);
    }


    /**
     * @param maxUncompressedDataPartOfFileBytes
     * @param uncompressedBlockSizeLimitBytes
     * @param avgUncompressedRecordSizeBytes
     * @throws IllegalArgumentException
     */
    public static FileStatsEstimator estimateUsingSizes(
            long maxUncompressedDataPartOfFileBytes, int uncompressedBlockSizeLimitBytes,
            int avgUncompressedRecordSizeBytes) {

        return new FileStatsEstimator(
                maxUncompressedDataPartOfFileBytes, uncompressedBlockSizeLimitBytes, avgUncompressedRecordSizeBytes
        );
    }

    /**
     * Another way (possible easier) to specify the constraints.
     *
     * @param avgNumBlocks
     * @param avgNumRecordsPerBlock
     * @param
     * @throws IllegalArgumentException
     */
    public static FileStatsEstimator estimateUsingCounts(
            int avgNumBlocks, int avgNumRecordsPerBlock, int avgUncompressedRecordSizeBytes) {

        return new FileStatsEstimator(
                (long) avgUncompressedRecordSizeBytes * avgNumRecordsPerBlock * avgNumBlocks,
                avgNumRecordsPerBlock * avgUncompressedRecordSizeBytes,
                avgUncompressedRecordSizeBytes
        );
    }

    public int estimatedTotalNumRecords() {
        return estimatedNumBlocks * estimatedNumRecordsPerBlock;
    }
}
