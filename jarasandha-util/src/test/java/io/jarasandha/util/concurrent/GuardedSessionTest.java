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

import io.jarasandha.util.test.AbstractTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class GuardedSessionTest extends AbstractTest {
    @Test
    public void testBasic() throws Exception {
        try {
            new GuardedSession<>(3 /*Concurrency*/, null);
            fail("Null item is not allowed");
        } catch (NullPointerException e) {
            log.debug("Expected", e);
        }

        final Integer givenItem = 999999;
        final GuardedSession<Integer> session = new GuardedSession<>(3 /*Concurrency*/, givenItem);

        //Can run borrow-back paired calls.
        for (int i = 0; i < 100; i++) {
            assertTrue(session.access(item -> assertSame(givenItem, item)));
        }

        //Test again.
        Integer item = session.borrow();
        if (item != null) {
            try {
                assertSame(givenItem, item);
            } finally {
                try {
                    session.back(/*Diff item*/ item * 10);
                    fail("Should've failed because the returned item is not the borrowed item");
                } catch (IllegalArgumentException e) {
                    log.debug("Expected", e);
                }
                session.back(item);
            }
        } else {
            fail("Item should not be null");
        }

        Integer discardedItem = session.discard();
        assertSame(givenItem, discardedItem);
    }

    @Test
    public void testConcurrency() throws Exception {
        final int concurrencyLimit = 3;
        final Integer givenItem = 999999;
        final GuardedSession<Integer> session = new GuardedSession<>(concurrencyLimit, givenItem);

        Integer item1 = null;
        Integer item2 = null;
        Integer item3 = null;
        Integer item4 = null;

        item1 = session.borrow();
        if (item1 == null) {
            fail("Item should not be null");
        }
        item2 = session.borrow();
        if (item2 == null) {
            fail("Item should not be null");
        }
        item3 = session.borrow();
        if (item3 == null) {
            fail("Item should not be null");
        }

        //No more room to borrow.
        item4 = session.borrow(100, TimeUnit.MILLISECONDS);
        assertNull("Item should not be null", item4);

        assertSame(givenItem, item1);
        assertSame(givenItem, item2);
        assertSame(givenItem, item3);
        session.back(item1);
        session.back(item2);
        session.back(item3);

        //Succeeds now.
        item4 = session.borrow();
        if (item4 == null) {
            fail("Item should not be null");
        }
        assertSame(givenItem, item4);
        session.back(item4);

        //Test discard and post discard states.
        Integer discardedItem = session.discard();
        assertSame(givenItem, discardedItem);
        //No-op.
        assertNull(session.discard());
        assertNull(session.borrow());
    }
}
