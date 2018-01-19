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
package io.jarasandha.store.api;

/**
 * Created by ashwin.jayaprakash.
 */
public class StoreException extends RuntimeException {
    private StoreException(String record) {
        super(record);
    }

    private StoreException(String record, Throwable cause) {
        super(record, cause);
    }

    /**
     * @param expression
     * @param errorRecordFormat The record used to throw the {@link StoreCorruptException} if the expression
     *                           evaluates to false.
     * @param recordParameters
     * @throws StoreCorruptException
     */
    public static void checkCorruption(boolean expression, String errorRecordFormat, Object... recordParameters) {
        if (!expression) {
            throw new StoreCorruptException(String.format(errorRecordFormat, recordParameters));
        }
    }

    public static class StoreFullException extends StoreException {
        public StoreFullException(String record) {
            super(record);
        }
    }

    public static class RecordTooBigException extends StoreException {
        public RecordTooBigException(String record) {
            super(record);
        }
    }

    public static class StoreWriteException extends StoreException {
        public StoreWriteException(String record, Throwable cause) {
            super(record, cause);
        }
    }

    public static class StoreNotOpenException extends StoreException {
        public StoreNotOpenException(String record) {
            super(record);
        }

        public StoreNotOpenException(String record, Throwable cause) {
            super(record, cause);
        }
    }

    /**
     * Generic exception.
     */
    public static class StoreReadException extends StoreException {
        StoreReadException(String record) {
            super(record);
        }

        public StoreReadException(String record, Throwable cause) {
            super(record, cause);
        }
    }

    /**
     * Specific readIndex exception.
     */
    public static class StoreCorruptException extends StoreReadException {
        public StoreCorruptException(String record) {
            super(record);
        }
    }
}
