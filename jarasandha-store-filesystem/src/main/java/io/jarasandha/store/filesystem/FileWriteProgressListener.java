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

import io.jarasandha.store.api.LogicalRecordLocation;
import io.jarasandha.store.api.StoreException;
import io.jarasandha.store.api.StoreWriteProgressListener;
import io.jarasandha.store.filesystem.shared.FileBlockInfo;
import io.jarasandha.store.filesystem.shared.FileId;
import io.jarasandha.store.filesystem.shared.FileInfo;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Accessors;
import net.jcip.annotations.NotThreadSafe;

import java.util.Deque;
import java.util.LinkedList;

/**
 * Tracks {@link #recordsInUnflushedBlock()}, {@link #completedFileBlockInfos()} and other progress.
 * <p>
 * If {@link #fileInfo()} is null, then writes have not started.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@NotThreadSafe
@Getter
@Accessors(fluent = true)
public abstract class FileWriteProgressListener
        implements StoreWriteProgressListener<FileId, FileInfo, FileBlockInfo> {

    private FileInfo fileInfo;
    private int recordsInUnflushedBlock;
    private Deque<FileBlockInfo> completedFileBlockInfos;
    private boolean closedOrFailed;
    private StoreException failureReason;

    /**
     * @param fileInfo
     * @return False by default.
     */
    @Override
    public boolean storeStarted(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
        this.completedFileBlockInfos = new LinkedList<>();
        return false;
    }

    /**
     * Calls {@link #handleNonNullRecordCopy(ByteBuf)}.
     *
     * @param recordLocation
     * @param recordCopyOrNull Null if {@link #storeStarted(FileInfo)} returned false. Otherwise a copy of the record,
     *                         which must be {@link ByteBuf#release() released} by {@link #handleNonNullRecordCopy(ByteBuf)}.
     */
    @Override
    public final void recordWritten(LogicalRecordLocation recordLocation, @CallerMustRelease ByteBuf recordCopyOrNull) {
        recordsInUnflushedBlock++;
        if (recordCopyOrNull != null) {
            handleNonNullRecordCopy(recordCopyOrNull);
        }
    }

    /**
     * Default implementation {@link ByteBuf#release() releases} the record.
     *
     * @param recordCopy
     */
    protected void handleNonNullRecordCopy(ByteBuf recordCopy) {
        recordCopy.release();
    }

    @Override
    public void blockCompleted(FileBlockInfo blockInfo) {
        completedFileBlockInfos.addLast(blockInfo);
        recordsInUnflushedBlock = 0;
    }

    @Override
    public void storeClosed() {
        recordsInUnflushedBlock = 0;
        closedOrFailed = true;
    }

    @Override
    public void storeFailed(StoreException exception) {
        recordsInUnflushedBlock = 0;
        closedOrFailed = true;
        failureReason = exception;
    }
}
