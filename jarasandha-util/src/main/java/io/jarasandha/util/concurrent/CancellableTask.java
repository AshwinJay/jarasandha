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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.function.Consumer;

/**
 * Utility class to help implement {@link Runnable}s that have state to be cleared at the end of their execution.
 * This is simpler than {@link CompletableFuture} and the {@link #cancel()} method is useful when the task has to
 * be cancelled - even when it has not been scheduled.
 * <p>
 * If the {@link ExecutorService#submit(Runnable)}  fails then the task can simply be {@link #cancel() cancelled} and
 * resources can be cleaned up.
 * <p>
 * Also see {@link #cancel()}.
 * <p>
 * The end could be due to:
 * <p>
 * - Success
 * <p>
 * - Failure - if the task has been {@link #cancel() cancelled} or the task executed but encountered an error.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class CancellableTask<T> implements Runnable {
    private static final int STATE_NOT_STARTED = 0;
    private static final int STATE_STARTED = 1;
    private static final int STATE_CANCELLED_NEVER_STARTED = 2;
    private static final int STATE_DONE_OR_INTERRUPTED_OR_ERROR = 3;

    private final Callable<T> mainTask;
    private final Consumer<T> onSuccessTask;
    private final Consumer<Throwable> onFailureTask;
    private final AtomicStampedReference<Thread> stateAndRunner;

    public CancellableTask(Callable<T> mainTask, Consumer<T> onSuccessTask, Consumer<Throwable> onFailureTask) {
        this.mainTask = mainTask;
        this.onSuccessTask = onSuccessTask;
        this.onFailureTask = onFailureTask;
        this.stateAndRunner = new AtomicStampedReference<>(null, STATE_NOT_STARTED);
    }

    /**
     * @throws IllegalStateException If the task was already done or cancelled before.
     */
    @Override
    public void run() {
        if (stateAndRunner.compareAndSet(null, Thread.currentThread(), STATE_NOT_STARTED, STATE_STARTED)) {
            try {
                final T result;
                try {
                    result = mainTask.call();
                } finally {
                    //Release here so as not to get interrupted while calling the success/failure task.
                    stateAndRunner.set(null, STATE_DONE_OR_INTERRUPTED_OR_ERROR);
                }
                onSuccessTask.accept(result);
            } catch (Throwable throwable) {
                onFailureTask.accept(throwable);
            }
        } else {
            throw new IllegalStateException("Task was already done or cancelled");
        }
    }

    /**
     * Guarantees that:
     * <p>
     * - The failure task is called with {@link CancellationException} if the task was never started.
     * <p>
     * - Or, cancels an already running task by interrupting that thread but not when it is running the failure/success
     * task. Like {@link FutureTask} this call does not wait for the main thread to complete and return, but merely
     * interrupts it and returns.
     */
    public void cancel() {
        if (stateAndRunner.compareAndSet(null, null, STATE_NOT_STARTED, STATE_CANCELLED_NEVER_STARTED)) {
            onFailureTask.accept(new CancellationException("Task has been cancelled"));
        } else {
            final Thread runner = stateAndRunner.getReference();
            if (runner != null) {
                runner.interrupt();
            }
        }
    }
}
