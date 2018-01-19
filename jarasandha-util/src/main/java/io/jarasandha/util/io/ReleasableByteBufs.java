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
package io.jarasandha.util.io;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;

/**
 * This class is meant for collecting {@link ByteBuf}s and then {@link ByteBuf#release() releasing} them
 * when {@link #close()} is called.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class ReleasableByteBufs implements AutoCloseable {
    private final Collection<? extends ByteBuf> byteBufs;

    public ReleasableByteBufs(ByteBuf... byteBufs) {
        this.byteBufs = newArrayList(byteBufs);
    }

    public ReleasableByteBufs(Collection<? extends ByteBuf> byteBufs) {
        this.byteBufs = byteBufs;
    }

    @Override
    public void close() {
        for (Iterator<? extends ByteBuf> iterator = byteBufs.iterator(); iterator.hasNext(); ) {
            ByteBuf byteBuf = iterator.next();
            if (byteBuf.refCnt() > 0) {
                try {
                    byteBuf.release();
                } catch (Throwable t) {
                    log.warn("Error occurred while releasing a " + ByteBuf.class.getSimpleName(), t);
                }
            }
            iterator.remove();
        }
    }
}
