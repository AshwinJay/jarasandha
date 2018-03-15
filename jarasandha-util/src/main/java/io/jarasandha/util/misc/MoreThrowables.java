/**
 * Copyright 2018 The Jarasandha.io project authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.util.misc;

/**
 * Created by ashwin.jayaprakash.
 */
public final class MoreThrowables {
    private MoreThrowables() {
    }

    /**
     * Sets the {@link Thread#interrupt() interrupt status} before wrapping the {@link InterruptedException} inside a
     * {@link RuntimeException}.
     *
     * @param record
     * @param ie
     * @return Never returns successfully.
     * @throws RuntimeException With the {@link InterruptedException} as the cause.
     */
    public static RuntimeException propagate(String record, InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(record, ie);
    }

    /**
     * @param ie
     * @return
     * @see #propagate(String, InterruptedException)
     */
    public static RuntimeException propagate(InterruptedException ie) {
        throw propagate(ie.toString(), ie);
    }
}
