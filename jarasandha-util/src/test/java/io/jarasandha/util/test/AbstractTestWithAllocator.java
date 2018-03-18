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
package io.jarasandha.util.test;

import ch.qos.logback.classic.Level;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;

/**
 * Creates a {@link ByteBufAllocator} and sets it to {@link ResourceLeakDetector.Level#PARANOID}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
public abstract class AbstractTestWithAllocator extends AbstractTest {
    protected ByteBufAllocator allocator;

    protected AbstractTestWithAllocator() {
    }

    protected AbstractTestWithAllocator(Level logLevel) {
        super(logLevel);
    }

    @Before
    public final void setUpAllocator() {
        allocator = new PooledByteBufAllocator(true);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE);
    }

    @After
    public final void gcAfterTest() {
        triggerGc();
    }
}
