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
package io.jarasandha.store.filesystem.cli;

import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

import java.io.PrintStream;
import java.util.List;
import java.util.function.Consumer;

public abstract class Commands {
    private Commands() {
    }

    public static void parse(String[] args, Object mainCommand, PrintStream out, Consumer<Object> lastCommandConsumer) {
        Object lastCommand = mainCommand;
        try {
            CommandLine commandLine = new CommandLine(mainCommand);
            List<CommandLine> commandLines = commandLine.parse(args);
            CommandLine lastCommandLine = commandLines.get(commandLines.size() - 1);
            lastCommand = lastCommandLine.getCommand();
            lastCommandConsumer.accept(lastCommand);
        } catch (ParameterException | IllegalArgumentException e) {
            out.println("Incorrect usage [" + e.getMessage() + "]\n");
            CommandLine.usage(lastCommand, out);
        } catch (Exception e) {
            e.printStackTrace(out);
        }
    }

    /**
     * The commands that are {@link #parse(String[], Object, PrintStream, Consumer) parsed} have to implement
     * {@link Runnable}.
     *
     * @param args
     * @param mainCommand
     * @param out
     */
    public static void parseAndRun(String[] args, Runnable mainCommand, PrintStream out) {
        parse(
                args, mainCommand, out,
                (Object o) -> {
                    if (o instanceof Runnable) {
                        ((Runnable) o).run();
                    } else {
                        throw new IllegalStateException(
                                "The command [" + o.getClass() + "] was expected to implement " +
                                        "[" + Runnable.class + "] but it does not");
                    }
                }
        );
    }
}
