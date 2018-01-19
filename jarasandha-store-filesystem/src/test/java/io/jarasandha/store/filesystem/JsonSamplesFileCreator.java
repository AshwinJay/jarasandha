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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.RandomBasedGenerator;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.jarasandha.store.filesystem.JsonSampleCreator.Record;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class JsonSamplesFileCreator {
    private static final String COMPACT = "compact";

    public static void main(String[] args) throws IOException {
        checkArgument(args.length == 3,
                "Expects 3 arguments [file, number-of-sample-records, " + COMPACT + "|pretty]");
        final String filePath = args[0];
        final File file = new File(filePath);
        final int numRecords = checkNotNull(Ints.tryParse(args[1]), "number-of-sample-records");
        checkArgument(numRecords >= 0);
        String format = Strings.nullToEmpty(args[2]).toLowerCase();
        final boolean compact = format.equals(COMPACT);

        final JsonFactory factory = new JsonFactory(new ObjectMapper());
        try (
                BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
                JsonGenerator generator = factory.createGenerator(bos, JsonEncoding.UTF8)
        ) {
            log.info("Writing at least [{}] sample records to [{}]", numRecords, filePath);

            if (!compact) {
                generator.setPrettyPrinter(new DefaultPrettyPrinter());
                generator.writeStartArray();
            }

            final RandomBasedGenerator uuidGenerator = Generators.randomBasedGenerator(ThreadLocalRandom.current());
            final int numOfUuidsToDuplicate = (int) (0.05 * numRecords);
            log.info("Num unique names [{}] out of [{}]", numOfUuidsToDuplicate, numRecords);
            final ArrayList<String> uuidNames = new ArrayList<>(numOfUuidsToDuplicate);
            for (int i = 0; i < numOfUuidsToDuplicate; i++) {
                String s = uuidGenerator.generate().toString();
                //Add twice.
                uuidNames.add(s);
                uuidNames.add(s);
            }
            Collections.shuffle(uuidNames);

            final ThreadLocalRandom tlsRandom = ThreadLocalRandom.current();
            for (int i = 0; (i < numRecords || !uuidNames.isEmpty()); i++) {
                Record record = JsonSampleCreator.newRecord(tlsRandom);
                String recurringName = (!uuidNames.isEmpty() && ThreadLocalRandom.current().nextBoolean())
                        ? uuidNames.remove(0)
                        : record.getName();
                record.setName(recurringName);
                generator.writeObject(record);
                if (compact) {
                    generator.flush();
                    bos.write('\n');
                }
                if (i > 0 && i % 1000 == 0) {
                    log.info("Written [{}] records so far", i);
                }
            }

            if (!compact) {
                generator.writeEndArray();
            }
        }

        log.info("Done and file size is [{}] bytes", file.length());
    }
}
