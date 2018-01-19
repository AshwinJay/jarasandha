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
package io.jarasandha.util.collection;

import io.jarasandha.util.test.AbstractTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class SparseFixedSizeListTest extends AbstractTest {
    @Test
    public void testBasic() {
        SparseFixedSizeList<String> list = new SparseFixedSizeList<>(0, null);

        assertEquals(0, list.size());
        for (String s : list) {
            fail("Iterator should not have iterated as size is 0");
        }

        try {
            list.get(0);
            Assert.fail();
        } catch (IndexOutOfBoundsException e) {
            log.trace("Expected", e);
        }

        try {
            list.add("hello");
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            log.trace("Expected", e);
        }
        try {
            list.add(0, "hello");
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            log.trace("Expected", e);
        }
        //Does not throw an exception as the actual removal depends upon finding the item first.
        assertFalse(list.remove("hello"));
        try {
            list.remove(0);
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            log.trace("Expected", e);
        }
    }

    @Test
    public void testSizeOne() {
        final String defaultValue = "Hello";
        SparseFixedSizeList<String> list = new SparseFixedSizeList<>(1, defaultValue);

        assertEquals(1, list.size());
        for (String s : list) {
            assertEquals(defaultValue, s);
        }
        assertEquals(defaultValue, list.get(0));

        final String newValue = "world";
        assertEquals(defaultValue, list.set(0, newValue));
        assertEquals(newValue, list.get(0));
        assertEquals(1, list.size());
        for (String s : list) {
            assertEquals(newValue, s);
        }
        assertEquals(newValue, list.get(0));

        try {
            //Finds the value but index removal fails.
            list.remove(newValue);
            fail();
        } catch (UnsupportedOperationException e) {
            log.trace("Expected", e);
        }
    }

    @Test
    public void testSparse() {
        final String defaultValue = "Hello";
        SparseFixedSizeList<String> list = new SparseFixedSizeList<>(100, defaultValue);

        assertEquals(100, list.size());
        for (String s : list) {
            assertEquals(defaultValue, s);
        }
        assertEquals(defaultValue, list.get(0));
        assertEquals(defaultValue, list.get(34));
        assertEquals(defaultValue, list.get(67));
        assertEquals(defaultValue, list.get(99));

        final String newValue = "world";
        list.set(49, newValue);
        assertEquals(newValue, list.get(49));
        assertEquals(100, list.size());
        int i = 0;
        for (String s : list) {
            assertEquals((i == 49) ? newValue : defaultValue, s);
            i++;
        }
    }
}
