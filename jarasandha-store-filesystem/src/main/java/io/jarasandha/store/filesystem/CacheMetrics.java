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
package io.jarasandha.store.filesystem;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * DropWizard Metrics on Caffeine cache statistics.
 * <p>
 * Created by ashwin.jayaprakash.
 */
class CacheMetrics implements StatsCounter {
    private final Collection<String> metricNames;
    private final Meter hits;
    private final Meter misses;
    private final Timer loadTotalTime;
    private final Meter loadSuccesses;
    private final Meter loadFailures;
    private final Meter evictions;
    private final Meter evictionWeights;

    CacheMetrics(MetricRegistry registry, String metricFamilyName) {
        checkNotNull(metricFamilyName);

        Builder<String> namesBuilder = ImmutableList.builder();
        hits = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/hits/counts"));
        misses = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/misses/counts"));
        loadTotalTime = registry.timer(addMetricName(namesBuilder, metricFamilyName + "/loads/time"));
        loadSuccesses = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/loads/successes/counts"));
        loadFailures = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/loads/failures/counts"));
        evictions = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/evictions/counts"));
        evictionWeights = registry.meter(addMetricName(namesBuilder, metricFamilyName + "/evictions/weights"));
        this.metricNames = namesBuilder.build();
    }

    private static String addMetricName(Builder<String> metricNamesBuilder, String name) {
        metricNamesBuilder.add(name);
        return name;
    }

    public Collection<String> getMetricNames() {
        return metricNames;
    }

    @Override
    public void recordHits(@Nonnegative int count) {
        hits.mark(count);
    }

    @Override
    public void recordMisses(@Nonnegative int count) {
        misses.mark(count);
    }

    @Override
    public void recordLoadSuccess(@Nonnegative long loadTime) {
        loadSuccesses.mark();
        loadTotalTime.update(loadTime, NANOSECONDS);
    }

    @Override
    public void recordLoadFailure(@Nonnegative long loadTime) {
        loadFailures.mark(loadTime);
        loadTotalTime.update(loadTime, NANOSECONDS);
    }

    @Override
    public void recordEviction() {
        evictions.mark();
    }

    @Override
    public void recordEviction(int weight) {
        evictions.mark();
        evictionWeights.mark(weight);
    }

    @Nonnull
    @Override
    public CacheStats snapshot() {
        return new CacheStats(
                hits.getCount(),
                misses.getCount(),
                loadSuccesses.getCount(),
                loadFailures.getCount(),
                loadTotalTime.getCount(),
                evictions.getCount(),
                evictionWeights.getCount()
        );
    }
}
