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
package io.jarasandha.util.concurrent;

import com.google.common.annotations.VisibleForTesting;
import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility to wrap a contented object ({@link I}) within predefined usage permits.
 * <p>
 * See {@link #access(long, TimeUnit, Consumer)}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@ThreadSafe
public final class GuardedSession<I> {
    private final int concurrentPermits;
    private final Semaphore semaphore;
    private volatile I item;

    /**
     * @param item
     * @see #GuardedSession(int, Object) Concurrency permit of 1.
     */
    public GuardedSession(I item) {
        this(1, item);
    }

    /**
     * @param concurrentPermits
     * @param item              Cannot be null.
     */
    public GuardedSession(int concurrentPermits, I item) {
        checkArgument(concurrentPermits > 0, "concurrentPermits");

        this.concurrentPermits = concurrentPermits;
        this.semaphore = new Semaphore(concurrentPermits);
        this.item = checkNotNull(item);
    }

    /**
     * @param consumer
     * @return If the item could be borrowed and then provided to the consumer.
     * @throws InterruptedException
     * @see #access(long, TimeUnit, Consumer)
     */
    public boolean access(Consumer<? super I> consumer) throws InterruptedException {
        return access(Integer.MAX_VALUE, TimeUnit.SECONDS, consumer);
    }

    /**
     * @param waitDuration
     * @param waitDurationUnits
     * @param consumer
     * @return If the item could be borrowed and then provided to the consumer.
     * @throws InterruptedException If the item could not be borrowed because:
     *                              <p>
     *                              - Session has already been {@link #discard() discarded}
     *                              <p>
     *                              - Or, the permit could not acquired within the given duration.
     */
    public boolean
    access(long waitDuration, TimeUnit waitDurationUnits, Consumer<? super I> consumer) throws InterruptedException {

        final I borrowed = borrow(waitDuration, waitDurationUnits);
        if (borrowed != null) {
            try {
                consumer.accept(borrowed);
                return true;
            } finally {
                back(borrowed);
            }
        }
        return false;
    }

    /**
     * @return
     * @throws InterruptedException
     * @see #borrow(long, TimeUnit)
     */
    @VisibleForTesting
    I borrow() throws InterruptedException {
        return borrow(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * <b>Usage:</b>
     * <p>
     * <pre>
     * Integer item = session.borrow();
     *  if (item != null) {
     *      try {
     *          ...
     *      } finally {
     *          session.back(item);
     *      }
     * }
     * </pre>
     *
     * @return Non-null item that is being guarded by this as yet {@link #discard() undiscarded} session. This returned
     * item <b>should not</b> be used outside the scope of this and {@link #back(Object)}.
     * <p>
     * Null if the session could not be started because:
     * <p>
     * - Session has already been {@link #discard() discarded}
     * <p>
     * - Or, the permit could not acquired within the given duration.
     * @throws InterruptedException
     */
    @VisibleForTesting
    I borrow(long waitDuration, TimeUnit waitDurationUnits) throws InterruptedException {
        long waitNanos = TimeUnit.NANOSECONDS.convert(waitDuration, waitDurationUnits);

        I i = null;
        while (/*Until not discarded*/ item != null && waitNanos > 0) {
            long startNanos = System.nanoTime();
            if (semaphore.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                i = item;
                break;
            }
            long endNanos = System.nanoTime();
            waitNanos -= (endNanos - startNanos);
        }
        return i;
    }

    /**
     * To be called only if the {@link #borrow()} succeeded.
     *
     * @param borrowedItem Object returned by {@link #borrow()}.
     */
    @VisibleForTesting
    void back(I borrowedItem) {
        checkArgument(borrowedItem == item);
        semaphore.release();
    }

    /**
     * Waits for all {@link #borrow(long, TimeUnit) borrowers} to safely return their items.
     *
     * @return The item <b>after</b> this session has been safely {@link #back(Object) stopped}. After this method
     * returns successfully, this session cannot be used.
     * <p>
     * Null if the session has already been discarded.
     * @throws InterruptedException
     */
    public I discard() throws InterruptedException {
        I i = null;
        while (/*Until not already discarded*/ item != null) {
            if (semaphore.tryAcquire(concurrentPermits /*Acquire all*/, 1000, TimeUnit.MILLISECONDS)) {
                i = item;
                //Publish nullification/discard.
                item = null;
                break;
            }
        }
        return i;
    }
}
