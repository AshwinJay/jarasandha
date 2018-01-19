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

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@Setter
@Accessors(chain = true)
public class ByteSize implements Validateable<ByteSize> {
    @Min(1)
    private long value;
    @NotNull
    @Valid
    private Unit unit;

    public static ByteSize of(long value, Unit unit) {
        return new ByteSize()
                .setUnit(unit)
                .setValue(value);
    }

    @Override
    public ByteSize validate() {
        checkArgument(value >= 0, "value [%s] has to be a positive integer", value);
        checkArgument(unit != null, "unit [%s] has to be provided %s",
                String.valueOf(unit), Arrays.asList(Unit.values()));
        return this;
    }

    /**
     * @return The total in bytes of {@link #getValue()} * {@link #getUnit()}.
     */
    public long toBytes() {
        return value * unit.toBytes();
    }

    public enum Unit {
        BYTES,
        KILOBYTES,
        MEGABYTES,
        GIGABYTES,
        TERABYTES,
        PETABYTES,
        EXABYTES;

        public long toBytes() {
            long size = 1;
            switch (this) {
                case EXABYTES:
                    size *= 1024;
                case PETABYTES:
                    size *= 1024;
                case TERABYTES:
                    size *= 1024;
                case GIGABYTES:
                    size *= 1024;
                case MEGABYTES:
                    size *= 1024;
                case KILOBYTES:
                    size *= 1024;
                case BYTES:
                    return size;
                default:
                    throw new IllegalArgumentException("Unrecognized unit");
            }
        }
    }
}
