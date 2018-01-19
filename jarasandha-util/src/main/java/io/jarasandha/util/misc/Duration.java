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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by ashwin.jayaprakash.
 */
@Getter
@Setter
@Accessors(chain = true)
public class Duration implements Validateable<Duration> {
    @Min(1)
    private long time;
    @NotNull
    @Valid
    private ChronoUnit unit;

    @Override
    public Duration validate() {
        checkArgument(time >= 0, "time [%s] has to be a positive integer", time);
        checkArgument(unit != null, "unit [%s] has to be provided %s",
                String.valueOf(unit), Arrays.asList(ChronoUnit.values()));
        return this;
    }

    public java.time.Duration toJavaDuration() {
        return java.time.Duration.of(time, unit);
    }
}
