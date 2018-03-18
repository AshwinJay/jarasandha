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

import io.netty.util.IllegalReferenceCountException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class ManagedReferenceTest {
    private Predicate<Integer> verifier;
    private Consumer<Integer> deallocator;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        verifier = Mockito.mock(Predicate.class);
        Mockito
                .when(verifier.test(anyInt()))
                .thenReturn(true);

        deallocator = Mockito.mock(Consumer.class);
    }

    @Test
    public void test_basic() {
        final int sampleInt = 123;
        final ManagedReference<Integer> mr = new ManagedReference<>(sampleInt, verifier, deallocator);
        assertEquals(1, mr.refCnt());
        assertEquals(new Integer(sampleInt), mr.actualRef());
        mr.release();
        assertEquals(0, mr.refCnt());

        verify(verifier, times(1)).test(sampleInt);
        verify(deallocator, times(1)).accept(sampleInt);
        assertNull(mr.actualRef());
    }

    @Test
    public void test_multipleRetains() {
        final int sampleInt = 123;
        final ManagedReference<Integer> mr = new ManagedReference<>(sampleInt, verifier, deallocator);
        assertEquals(new Integer(sampleInt), mr.actualRef());
        assertEquals(1, mr.refCnt());
        mr.retain();
        assertEquals(2, mr.refCnt());
        mr.release();
        assertEquals(1, mr.refCnt());
        mr.retain(3);
        assertEquals(4, mr.refCnt());
        mr.release(4);
        assertEquals(0, mr.refCnt());

        verify(verifier, times(3 /*ctor + with retain + with retain(3)*/)).test(sampleInt);
        verify(deallocator, times(1)).accept(sampleInt);
        assertNull(mr.actualRef());
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void test_failedVerification_throwsIllegalRef() {
        new ManagedReference<>(123, integer -> false, deallocator);
    }

    @Test
    public void test_basic_autocloseable() {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final Stream<Integer> sampleStream = Stream.of(123).onClose(() -> closed.set(true));
        final ManagedReference<Stream<Integer>> mr = ManagedReference.newManagedReference(sampleStream);
        assertFalse(closed.get());
        assertEquals(1, mr.refCnt());
        assertSame(sampleStream, mr.actualRef());
        mr.release();
        assertEquals(0, mr.refCnt());

        assertTrue(closed.get());
        assertNull(mr.actualRef());
    }
}
