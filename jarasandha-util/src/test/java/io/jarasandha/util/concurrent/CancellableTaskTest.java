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
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class CancellableTaskTest {
    @Test
    public void testSuccess() {
        final AtomicInteger successCalled = new AtomicInteger();
        final AtomicReference<Throwable> errorCalled = new AtomicReference<>();

        CancellableTask<Integer> task = new CancellableTask<>(() -> 100, successCalled::set, errorCalled::set);

        task.run();
        assertEquals(100, successCalled.get());
        assertNull(errorCalled.get());

        //No effect.
        task.cancel();
    }

    @Test(expected = IllegalStateException.class)
    public void testNeverStarted() {
        final AtomicInteger successCalled = new AtomicInteger(-2);
        final AtomicReference<Throwable> errorCalled = new AtomicReference<>();

        CancellableTask<Integer> task = new CancellableTask<>(() -> 100, successCalled::set, errorCalled::set);

        task.cancel();
        //Never called.
        assertEquals(-2, successCalled.get());
        assertTrue(errorCalled.get() instanceof CancellationException);

        //Will throw exception.
        task.run();
    }

    @Test
    public void testInterrupted() throws InterruptedException {
        final AtomicInteger successCalled = new AtomicInteger(-2);
        final AtomicReference<Throwable> errorCalled = new AtomicReference<>();

        final CountDownLatch taskStartLatch = new CountDownLatch(1);
        final CountDownLatch taskEndLatch = new CountDownLatch(1);
        CancellableTask<Integer> task = new CancellableTask<>(
                () -> {
                    taskStartLatch.countDown();
                    //Never return.
                    Thread.sleep(Long.MAX_VALUE);
                    return 100;
                },
                successCalled::set,
                throwable -> {
                    errorCalled.set(throwable);
                    taskEndLatch.countDown();
                }
        );

        final Thread runnerThread = new Thread(task);
        runnerThread.start();
        taskStartLatch.await();

        task.cancel();
        //Wait for the cancellation to complete.
        taskEndLatch.await();
        //Never called.
        assertEquals(-2, successCalled.get());
        assertTrue(errorCalled.get() instanceof InterruptedException);
    }
}
