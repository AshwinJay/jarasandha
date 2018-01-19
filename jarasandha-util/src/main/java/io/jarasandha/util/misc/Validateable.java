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

/**
 * Suitable for use like this: {@code Foo f = new Foo(param1, param2, param3).validate();}
 * <p>
 * Created by ashwin.jayaprakash.
 */
public interface Validateable<V> {
    /**
     * @return {@code this} if valid. Throw {@link RuntimeException} if not valid.
     */
    @CostlyOperation("Can be assumed to be expensive in most cases. " +
            "Otherwise validation would've been done in the constructor")
    default V validate() {
        throw new IllegalArgumentException();
    }
}
