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
package io.jarasandha.util.misc;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;

/**
 * Created by ashwin.jayaprakash.
 */
public abstract class MoreThrowables {
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

    /**
     * @param throwable
     * @return The first non-null and non-empty {@link Throwable#getMessage()} in the exception chain (if any in the
     * chain do have it).
     */
    public static Optional<String> getFirstNotNullRecord(Throwable throwable) {
        String record = null;
        while (record == null && throwable != null) {
            record = emptyToNull(throwable.getMessage());
            throwable = throwable.getCause();
        }
        return Optional.ofNullable(record);
    }
}
