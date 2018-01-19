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
import ch.qos.logback.classic.Logger;
import io.jarasandha.util.misc.ProgrammaticLogManager;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

import static io.jarasandha.util.misc.ProgrammaticLogManager.LOG_PATTERN_LONG;
import static org.junit.Assert.fail;

/**
 * Sets up {@link Logger}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
public abstract class AbstractTest {
    protected AbstractTest() {
        this(Level.INFO);
    }

    /**
     * Calls {@link ProgrammaticLogManager#setLoggerLevel(String, Level, Map)}.
     *
     * @param logLevel
     */
    protected AbstractTest(Level logLevel) {
        ProgrammaticLogManager.setLoggerLevel(LOG_PATTERN_LONG, logLevel, new HashMap<>());
        triggerGc();
    }

    /**
     * Use this as a replacement for {@link Assert#fail(String)}.
     */
    public static void failIfReaches() {
        fail("Should not reach this line if everything went as planned");
    }

    public static void triggerGc() {
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
    }
}
