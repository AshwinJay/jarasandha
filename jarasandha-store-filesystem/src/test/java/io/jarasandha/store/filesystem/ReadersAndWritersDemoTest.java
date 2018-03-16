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

import com.codahale.metrics.MetricRegistry;
import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.api.StoreException.StoreFullException;
import io.jarasandha.store.api.StoreReader;
import io.jarasandha.store.api.StoreWriter;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.ByteSize.Unit;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static io.jarasandha.util.misc.Formatters.format;
import static io.jarasandha.util.misc.Uuids.UUID_BYTES_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * A simple test that demonstrates the use of the high level APIs to write to and read from the files:
 * <p>
 * - {@link Writers}
 * <p>
 * - {@link StoreWriter}
 * <p>
 * - {@link Readers}
 * <p>
 * - {@link StoreReader}
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class ReadersAndWritersDemoTest extends AbstractTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Files allFiles;

    private ArrayList<SampleRecord> sampleRecordsToVerify;

    @Before
    public void setUp() throws IOException {
        allFiles = new Files(folder.newFolder());
        sampleRecordsToVerify = new ArrayList<>();
    }

    @Test
    public void testDemo() {
        doFileWrites();
        doFileReads();
    }

    private void doFileWrites() {
        //Create a "writers" object to assist with the creation of individual "writer" objects.
        try (
                Writers writers = new Writers(
                        allFiles, "jara", new PooledByteBufAllocator(true), NoOpFileWriteProgressListener::new
                )
        ) {
            final int uncompressedBlockBytesLimit = 20 * UUID_BYTES_SIZE;
            final int fileSizeBytesLimit = 12 * uncompressedBlockBytesLimit;
            WriterParameters writerParameters = new WriterParameters()
                    .blocksCompressed(true)
                    .indexCompressed(true)
                    .uncompressedBytesPerBlockLimit(uncompressedBlockBytesLimit)
                    .fileSizeBytesLimit(fileSizeBytesLimit)
                    .validate();

            int numRecordsSoFar = 0;
            //Create a few writers and write records into them.
            for (int i = 1; i <= 5; i++) {
                try (StoreWriter<FileId> storeWriter = writers.newStoreWriter(writerParameters)) {
                    FileId storeId = storeWriter.getStoreId();

                    log.info("Starting [{}] writer [{}]", i, storeId);
                    while (true) {
                        //Write as many records as will fit in the file.
                        //The records are of fixed length and generated randomly (UUIDs).
                        UUID uuid = Uuids.newUuid();
                        final ByteBuf randomlyGeneratedRecord = Uuids.byteBufFromUuid(uuid);
                        try {
                            try {
                                LogicalRecordLocation recordLocation = storeWriter.write(randomlyGeneratedRecord);
                                captureSample(numRecordsSoFar++, storeId, uuid, recordLocation);

                                //Store the record location for future retrieval. Or, just log it in this case.
                                log.trace("Block [{}], record [{}]",
                                        recordLocation.getPositionOfBlock(),
                                        recordLocation.getPositionOfRecordInBlock());
                            } catch (StoreFullException e) {
                                log.warn("File seems to have reached its limit [{}]", e.getMessage());
                                break;
                            }
                        } finally {
                            randomlyGeneratedRecord.release();
                        }
                    }
                    log.info("Completed [{}] writer", i);
                }
            }

            //Print the names of all the files that were created.
            writers
                    .showAllFiles()
                    .forEach(
                            pair -> log.info("File [{}] has [{}] bytes", pair.getTwo(), format(pair.getOne().length()))
                    );
        }
    }

    private void captureSample(int numRecordsSoFar, FileId storeId, UUID uuid, LogicalRecordLocation recordLocation) {
        final int maxSamples = 50;
        final int size = sampleRecordsToVerify.size();
        if (size >= maxSamples) {
            int positionToReplace = ThreadLocalRandom.current().nextInt(numRecordsSoFar);
            if (positionToReplace < size) {
                sampleRecordsToVerify.set(positionToReplace, new SampleRecord(storeId, recordLocation, uuid));
            }
        } else {
            sampleRecordsToVerify.add(new SampleRecord(storeId, recordLocation, uuid));
        }
    }

    private void doFileReads() {
        //Just shake it up a little.
        Collections.shuffle(sampleRecordsToVerify, ThreadLocalRandom.current());
        log.info("Attempting to verify [{}] samples", sampleRecordsToVerify.size());

        //Configure the readers so that it caches uncompressed blocks, even file metadata for fast repeat access.
        ReadersParameters readersParameters = new ReadersParameters()
                .files(allFiles)
                .fileEventListener(new DefaultFileEventListener())
                .blockEventListener(key -> {
                })
                .allocator(new PooledByteBufAllocator(true))
                .metricRegistry(new MetricRegistry())
                .expireAfterAccess(Duration.ofSeconds(0))
                .maxOpenFiles(10)
                .maxTotalUncompressedBlockBytes(128 * Unit.KILOBYTES.toBytes());

        try (Readers readers = new Readers(readersParameters)) {
            for (SampleRecord sampleRecord : sampleRecordsToVerify) {
                //Read the sample records and compare them with the expected value.
                log.info("Reading [{}] from file [{}] {}", sampleRecord.recordLocation, sampleRecord.fileId, readers);

                try {
                    final ByteBuf record = readers.readRecord(sampleRecord.fileId, sampleRecord.recordLocation);
                    try {
                        byte[] bytes = ByteBufUtil.getBytes(record);
                        UUID actual = Uuids.uuidFromBytes(bytes);
                        UUID expected = sampleRecord.uuid;
                        assertEquals(expected, actual);
                    } finally {
                        record.release();
                    }
                } catch (Exception e) {
                    log.error("FATAL", e);
                }
            }
        }

        log.info("Completed verifying samples");
    }

    private static class SampleRecord {
        private final FileId fileId;
        private final LogicalRecordLocation recordLocation;
        private final UUID uuid;

        private SampleRecord(FileId fileId, LogicalRecordLocation recordLocation, UUID uuid) {
            this.fileId = fileId;
            this.recordLocation = recordLocation;
            this.uuid = uuid;
        }
    }
}
