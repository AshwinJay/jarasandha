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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Created by ashwin.jayaprakash.
 */
public class FileHeaderCodecTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRoundTrip() throws IOException {
        File file = folder.newFile();
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), WRITE, READ)) {
            //Write.
            FileHeaderCodec.writeHeader(fileChannel, ByteBufAllocator.DEFAULT);

            //Write some data.
            final ByteBuf data = Unpooled.buffer();
            try {
                data.writeBytes("test 123 abc".getBytes(UTF_8));
                data.readBytes(fileChannel, data.readableBytes());
            } finally {
                data.release();
            }

            //Read and ensure there are no errors.
            FileHeaderCodec.readHeader(fileChannel, ByteBufAllocator.DEFAULT);
        }
    }
}
