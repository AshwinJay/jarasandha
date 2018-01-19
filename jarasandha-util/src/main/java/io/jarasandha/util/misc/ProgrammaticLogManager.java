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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

public abstract class ProgrammaticLogManager {
    public static final String LOG_PATTERN_LONG = "[%date{ISO8601}] [%-5level] [%thread] [%logger{16}] - %msg%n";
    public static final String LOG_PATTERN_SMALL = "[%relative] %msg%n";

    private ProgrammaticLogManager() {
    }

    /**
     * @param logPattern       {@link #LOG_PATTERN_LONG} or {@link #LOG_PATTERN_SMALL}.
     * @param appendedLogLevel
     * @param loggersAndLevels Specific loggers whose {@link Level log levels} are greater or same as the
     *                         {@code appendedLogLevel}.
     */
    @SuppressWarnings("unchecked")
    public static void setLoggerLevel(String logPattern, Level appendedLogLevel, Map<String, Level> loggersAndLevels) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        PatternLayoutEncoder ple = new PatternLayoutEncoder();
        ple.setPattern(logPattern);
        ple.setContext(loggerContext);
        ple.start();

        ConsoleAppender console = new ConsoleAppender();
        console.setEncoder(ple);
        console.setContext(loggerContext);
        ThresholdFilter filter = new ThresholdFilter();
        filter.setLevel(appendedLogLevel.toString());
        console.addFilter(filter);
        console.start();

        Map<String, Level> map = loggersAndLevels;
        if (map == null || map.isEmpty()) {
            map = new HashMap<>();
            map.put(ROOT_LOGGER_NAME, appendedLogLevel);
        }
        map.putIfAbsent(ROOT_LOGGER_NAME, Level.WARN);

        for (Entry<String, Level> entry : map.entrySet()) {
            Logger logger = loggerContext.getLogger(entry.getKey());
            logger.setLevel(entry.getValue());
            //Only for the root.
            if (ROOT_LOGGER_NAME.equals(entry.getKey())) {
                logger.addAppender(console);
            }
        }
    }
}
