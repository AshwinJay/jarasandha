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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import io.jarasandha.util.misc.Validateable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@EqualsAndHashCode
public class LogicalRecordLocation implements Comparable<LogicalRecordLocation>, Validateable<LogicalRecordLocation> {
    private final int positionOfBlock;
    private final int positionOfRecordInBlock;

    public LogicalRecordLocation(int positionOfBlock, int positionOfRecordInBlock) {
        this.positionOfBlock = positionOfBlock;
        this.positionOfRecordInBlock = positionOfRecordInBlock;
    }

    @Override
    public LogicalRecordLocation validate() {
        checkArgument(positionOfBlock >= 0, "positionOfBlock");
        checkArgument(positionOfRecordInBlock >= 0, "positionOfRecordInBlock");
        return this;
    }

    @Override
    public int compareTo(LogicalRecordLocation that) {
        return ComparisonChain
                .start()
                .compare(positionOfBlock, that.getPositionOfRecordInBlock())
                .compare(positionOfRecordInBlock, that.getPositionOfRecordInBlock())
                .result();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
                .add("positionOfBlock", positionOfBlock)
                .add("positionOfRecordInBlock", positionOfRecordInBlock)
                .toString();
    }
}
