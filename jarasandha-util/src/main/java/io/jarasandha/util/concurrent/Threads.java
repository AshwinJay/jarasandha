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
package io.jarasandha.util.concurrent;

/**
 * Created by ashwin.jayaprakash.
 */
public final class Threads {
    private Threads() {
    }

    /**
     * Renames the {@link Thread#setName(String) thread} before running the given {@link Runnable}.
     * Once the execution is done, the thread's name is reverted back to its {@link Thread#getName() original} name.
     *
     * @param name
     * @param runnable
     */
    public static void renameThreadAndRun(String name, Runnable runnable) {
        final Thread thread = Thread.currentThread();
        final String oldThreadName = thread.getName();
        thread.setName(name);
        try {
            runnable.run();
        } finally {
            thread.setName(oldThreadName);
        }
    }
}
