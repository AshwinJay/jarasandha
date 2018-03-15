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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.api.StoreException.StoreReadException;
import io.jarasandha.store.filesystem.shared.FileBlockPosition;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.util.misc.MoreThrowables;
import io.jarasandha.util.misc.Uuids;
import io.jarasandha.util.test.AbstractTestWithAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.jarasandha.store.filesystem.DebugFileWriter.randomNewFileId;
import static io.jarasandha.util.collection.ImmutableBitSet.newBitSetWithAllPositionsTrue;
import static io.jarasandha.util.io.ByteBufs.SIZE_BYTES_CHECKSUM;
import static io.jarasandha.util.misc.Uuids.UUID_BYTES_SIZE;
import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class ReadersTest extends AbstractTestWithAllocator {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testOpsOnBlankFile() throws IOException {
        final Files files = new Files(folder.newFolder());
        final Readers readers = new Readers(
                new ReadersParameters()
                        .files(files)
                        .fileEventListener(new DefaultFileEventListener())
                        .blockEventListener(key -> {
                        })
                        .allocator(allocator)
                        .metricRegistry(new MetricRegistry())
                        .expireAfterAccess(Duration.ofSeconds(0))
                        .maxOpenFiles(0)
                        .maxTotalUncompressedBlockBytes(0)
        );

        final FileId fileId = new FileId(Uuids.newUuid(), "dat");

        //Read non-existent block.
        {
            try {
                readers.readBlock(fileId, -34);
                failIfReaches();
            } catch (Exception e) {
                log.debug("Expected", e);
                assertTrue(Throwables.getRootCause(e) instanceof NoSuchFileException);
            }

            try {
                readers.readBlock(fileId, 14);
                failIfReaches();
            } catch (Exception e) {
                log.debug("Expected", e);
                assertTrue(Throwables.getRootCause(e) instanceof NoSuchFileException);
            }
        }

        //Read non-existent record.
        {
            try {
                readers.readRecord(fileId, new LogicalRecordLocation(12, 344));
                failIfReaches();
            } catch (Exception e) {
                log.debug("Expected", e);
                assertTrue(Throwables.getRootCause(e) instanceof NoSuchFileException);
            }
        }

        //Remove file.
        {
            readers.release(fileId);
            //No-op. Already removed.
            readers.release(fileId);
        }

        readers.close();
        //No-op.
        readers.close();
    }

    @AllArgsConstructor
    private static class TestData {
        final File file;
        final FileId fileId;
        final Map<LogicalRecordLocation, ByteBuf> bufsCreated = new HashMap<>();
    }

    @AllArgsConstructor
    private static class AccessData {
        final FileId fileId;
        final LogicalRecordLocation llr;
        final ByteBuf dataToVerify;
    }

    private Pair<TestData[], List<AccessData>> prepareData(int numFiles, int numRecordsPerFile, Files files)
            throws IOException {
        //Prepare multiple files with data.
        final TestData[] testData = new TestData[numFiles];
        for (int i = 0; i < testData.length; i++) {
            Pair<File, FileId> pair = files.newFile("dat");
            assertTrue(pair.getOne().createNewFile());
            testData[i] = new TestData(pair.getOne(), pair.getTwo());

            //Fill with data.
            assertTrue(testData[i].file.delete());
            try (
                    FileWriter writer = new FileWriter(
                            randomNewFileId(), testData[i].file, 1024, UUID_BYTES_SIZE + SIZE_BYTES_CHECKSUM,
                            allocator, new NoOpFileWriteProgressListener())
            ) {
                for (int m = 0; m < numRecordsPerFile; m++) {
                    ByteBuf bb = Uuids.newUuidByteBuf();
                    //Copy before save() which will consume the buf.
                    ByteBuf expected = bb.slice();
                    testData[i].bufsCreated.put(writer.write(bb), expected);
                }
            }
            log.info("File [{}] created and has size [{}]",
                    testData[i].file.getAbsolutePath(), testData[i].file.length());
        }

        //Shuffle the test data to see if the caches work.
        final List<AccessData> accessData = new LinkedList<>();
        Arrays.stream(testData).forEach(td -> {
            td.bufsCreated.forEach((logicalRecordLocation, byteBuf) -> {
                accessData.add(new AccessData(td.fileId, logicalRecordLocation, byteBuf));
            });
        });
        //Shuffle to make sure that the caches are evicting but still working.
        Collections.shuffle(accessData);
        assertEquals(numFiles * numRecordsPerFile, accessData.size());
        log.info("Testing total of [{}] records from [{}] files", accessData.size(), testData.length);

        return pair(testData, accessData);
    }

    @Test
    public void testRecordAndBlockReads() throws IOException {
        final int numFiles = 4;
        final int numRecordsPerFile = 10;
        final Files files = new Files(folder.newFolder());
        final BlockingQueue<FileId> fileRemovalIndicator = new LinkedBlockingQueue<>();
        final DefaultFileEventListener fileEventListener = new DefaultFileEventListener() {
            @Override
            public void onMiss(FileId fileId, File missingFile) {
                super.onMiss(fileId, missingFile);
                log.info("Loading [{}]", fileId);
            }

            @Override
            public void onRemove(FileId fileId, File removableFile) {
                fileRemovalIndicator.offer(fileId);
                log.info("Released [{}]", fileId);
            }
        };
        final BlockingQueue<FileBlockPosition> blockRemovalIndicator = new LinkedBlockingQueue<>();
        final ReadersParameters parameters = new ReadersParameters()
                .files(files)
                .fileEventListener(fileEventListener)
                .blockEventListener(blockRemovalIndicator::offer)
                .allocator(allocator)
                .metricRegistry(new MetricRegistry())
                .expireAfterAccess(Duration.ofHours(1))
                .maxOpenFiles(numFiles)
                .maxTotalUncompressedBlockBytes(MAX_VALUE);
        final Readers readers = new Readers(parameters);

        final Pair<TestData[], List<AccessData>> pair = prepareData(numFiles, numRecordsPerFile, files);
        final TestData[] testData = pair.getOne();
        final List<AccessData> accessData = pair.getTwo();

        accessData.forEach(ad -> {
            log.info("Testing file [{}] and llr [{}]", ad.fileId, ad.llr);

            //Read the entire block to which this record belongs.
            ByteBuf blockBb = readers.readBlock(ad.fileId, ad.llr.getPositionOfBlock());
            assertEquals(2 /*1 internal cache ref as it has not expired yet + 1 this*/, blockBb.refCnt());

            //Test record is present.
            ByteBuf actualRecordBb = readers.readRecord(ad.fileId, ad.llr);
            assertTrue(ByteBufUtil.compare(actualRecordBb, ad.dataToVerify) == 0);
            assertEquals(1, actualRecordBb.refCnt());
            actualRecordBb.release();

            //Clear and check again to ensure that the cache will load it again.
            final Runnable removalsTester = () -> {
                readers.release(ad.fileId /*Clear the cache*/);
                readers.cleanUp();
                //Make sure that the removal listener was called.
                try {
                    final FileId removedFileId = fileRemovalIndicator.poll(1, MINUTES);
                    assertEquals(ad.fileId, removedFileId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    final FileBlockPosition removedBlock = blockRemovalIndicator.poll(1, MINUTES);
                    assertEquals(ad.fileId, removedBlock.toFileId());
                    assertEquals(ad.llr.getPositionOfBlock(), removedBlock.getBlockPosition());
                    log.info("Verified removal of file [{}] and block [{}]", ad.fileId,
                            removedBlock.getBlockPosition());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            removalsTester.run();

            assertEquals(1/*No internal cache ref + 1 this*/, blockBb.refCnt());
            blockBb.release();

            //Test record is present.
            actualRecordBb = readers.readRecord(ad.fileId, ad.llr);
            assertTrue(ByteBufUtil.compare(actualRecordBb, ad.dataToVerify) == 0);
            assertEquals(1, actualRecordBb.refCnt());
            actualRecordBb.release();

            ad.dataToVerify.release();

            //Clear and check again to ensure that the cache will load it again.
            removalsTester.run();
        });

        //Delete the files and empty the caches.
        for (TestData td : testData) {
            assertTrue(td.file.delete());
            readers.release(td.fileId);
        }
        //Then try to load them.
        try {
            readers.readIndex(accessData.get(0).fileId, newBitSetWithAllPositionsTrue());
        } catch (StoreReadException e) {
            log.debug("Expected", e);
        }

        readers.close();
    }

    @Test
    public void testBlocksWith1Record_withFileHandler() throws IOException {
        final int numFiles = 1;
        final int numRecordsPerFile = 1;
        final File dir = folder.newFolder();
        final Files files = new Files(dir);
        final File movedTo = folder.newFile();
        final AtomicInteger missCount = new AtomicInteger();
        final AtomicInteger removeCount = new AtomicInteger();
        //Latch needed here because Caffeine removes asynchronously.
        final CountDownLatch removeWaiter = new CountDownLatch(1);

        final FileEventListener handler = new DefaultFileEventListener() {
            @Override
            public void onMiss(FileId fileId, File missingFile) {
                //Don't move the first time when the file is created manually.
                if (removeCount.get() > 0) {
                    log.info("Moving back [{}] to [{}] on miss", movedTo.getAbsolutePath(),
                            missingFile.getAbsolutePath());
                    assertTrue(movedTo.renameTo(missingFile));
                    missCount.incrementAndGet();
                }
            }

            @Override
            public void onRemove(FileId fileId, File removableFile) {
                log.info("Moving [{}] to [{}] on removal",
                        removableFile.getAbsolutePath(), movedTo.getAbsolutePath());
                assertTrue(removableFile.renameTo(movedTo));
                removeCount.incrementAndGet();
                removeWaiter.countDown();
            }
        };
        final ReadersParameters parameters = new ReadersParameters()
                .files(files)
                .fileEventListener(handler)
                .blockEventListener(key -> {
                })
                .allocator(allocator)
                .metricRegistry(new MetricRegistry())
                .expireAfterAccess(Duration.ofHours(1))
                .maxOpenFiles(numFiles)
                .maxTotalUncompressedBlockBytes(MAX_VALUE);
        final Readers readers = new Readers(parameters);

        final Pair<TestData[], List<AccessData>> pair = prepareData(numFiles, numRecordsPerFile, files);
        final TestData[] testData = pair.getOne();
        final List<AccessData> accessData = pair.getTwo();

        assertEquals(1 /*Only 1 block*/,
                readers.readIndex(testData[0].fileId, newBitSetWithAllPositionsTrue()).blocks().size());
        assertEquals(1 /*1 record*/, accessData.size());

        ByteBuf record = readers.readRecord(testData[0].fileId, accessData.get(0).llr);
        assertTrue(ByteBufUtil.compare(record, accessData.get(0).dataToVerify) == 0);
        assertEquals(1, record.refCnt());

        ByteBuf block = readers.readBlock(testData[0].fileId, 0 /*First and only block*/);
        //The entire block has just 1 record.
        assertTrue(ByteBufUtil.compare(record, block) == 0);
        block.release();

        //Remove the file which will trigger the FileHandler.
        readers.release(testData[0].fileId);
        readers.cleanUp();
        try {
            removeWaiter.await();
        } catch (InterruptedException e) {
            throw MoreThrowables.propagate(e);
        }

        //Trigger a load again.
        record = readers.readRecord(testData[0].fileId, accessData.get(0).llr);
        assertTrue(ByteBufUtil.compare(record, accessData.get(0).dataToVerify) == 0);
        assertEquals(1, record.refCnt());
        assertEquals(1, missCount.get());
        assertEquals(1, removeCount.get());

        record.release();
    }
}
