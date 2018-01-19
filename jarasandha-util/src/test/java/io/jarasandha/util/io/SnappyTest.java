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
package io.jarasandha.util.io;

import io.jarasandha.util.test.AbstractTest;
import io.netty.buffer.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.jarasandha.util.misc.Formatters.format;
import static org.junit.Assert.assertEquals;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class SnappyTest {
    private static final byte[] TEST_BYTES = ("" +
            "[32] But I must explain to you how all this mistaken idea of denouncing of a\n" +
            "pleasure and praising pain was born and I will give you a complete account of\n" +
            "the system, and expound the actual teachings of the great explorer of the truth,\n" +
            "the master-builder of human happiness. No one rejects, dislikes, or avoids\n" +
            "pleasure itself, because it is pleasure, but because those who do not know how\n" +
            "to pursue pleasure rationally encounter consequences that are extremely painful.\n" +
            "Nor again is there anyone who loves or pursues or desires to obtain pain of\n" +
            "itself, because it is pain, but occasionally circumstances occur in which toil\n" +
            "and pain can procure him some great pleasure. To take a trivial example, which\n" +
            "of us ever undertakes laborious physical exercise, except to obtain some\n" +
            "advantage from it? But who has any right to find fault with a man who chooses to\n" +
            "enjoy a pleasure that has no annoying consequences, or one who avoids a pain\n" +
            "that produces no resultant pleasure?\n" +
            "\n" +
            "[33] On the other hand, we denounce with righteous indignation and dislike men\n" +
            "who are so beguiled and demoralized by the charms of pleasure of the moment, so\n" +
            "blinded by desire, that they cannot foresee the pain and trouble that are bound\n" +
            "to ensue; and equal blame belongs to those who fail in their duty through\n" +
            "weakness of will, which is the same as saying through shrinking from toil and\n" +
            "pain. These cases are perfectly simple and easy to distinguish. In a free hour,\n" +
            "when our power of choice is untrammeled and when nothing prevents our being able\n" +
            "to do what we like best, every pleasure is to be welcomed and every pain\n" +
            "avoided. But in certain circumstances and owing to the claims of duty or the\n" +
            "obligations of business it will frequently occur that pleasures have to be\n" +
            "repudiated and annoyances accepted. The wise man therefore always holds in these\n" +
            "matters to this principle of selection: he rejects pleasures to secure other\n" +
            "greater pleasures, or else he endures pains to avoid worse pains ohs.\n")
            .getBytes(StandardCharsets.UTF_8);

    @After
    public void afterTest() {
        AbstractTest.triggerGc();
    }

    @Test
    public void testSnappyLarge() {
        runTest(TEST_BYTES, 9_371);
    }

    @Test
    public void testSnappyMedium() {
        runTest(TEST_BYTES, 1);
    }

    @Test
    public void testSnappySmall() {
        runTest(new byte[]{1, 3, 40, -3, 47}, 1);
    }

    private static void runTest(byte[] testBytes, int numTestByteComponents) {
        final ByteBufAllocator allocator = new PooledByteBufAllocator(true);

        final CompositeByteBuf rawByteBuf = allocator.compositeBuffer();
        for (int i = 0; i < numTestByteComponents /*Some sufficiently "large" number for the test*/; i++) {
            rawByteBuf.addComponents(true, Unpooled.wrappedBuffer(testBytes));
        }
        final int rawByteBufLength = rawByteBuf.readableBytes();
        log.info("Data size [{}]", format(rawByteBufLength));

        final long rawByteBufChecksum = ByteBufs.calculateChecksum(rawByteBuf);
        final long slowRawByteBufChecksum = ByteBufs.calculateChecksumNio(rawByteBuf);
        log.info("Checksum production: [{}], test: [{}]", rawByteBufChecksum, slowRawByteBufChecksum);
        assertEquals(rawByteBufChecksum, slowRawByteBufChecksum);

        try {
            //Compress.
            final ByteBuf compressedByteBuf;
            compressedByteBuf = ByteBufs.compress(allocator, rawByteBuf);
            log.info("Raw [{}], compressed [{}]", format(rawByteBufLength), format(compressedByteBuf.readableBytes()));

            //Decompress.
            final ByteBuf newRawByteBuf;
            try {
                newRawByteBuf = ByteBufs.decompress(allocator, compressedByteBuf);
            } finally {
                compressedByteBuf.release();
            }
            try {
                log.info("Decompressed [{}], raw [{}]",
                        format(newRawByteBuf.readableBytes()), format(rawByteBufLength));

                //Ensure the original and the new decompressed sizes are equal.
                assertEquals(rawByteBufLength, newRawByteBuf.readableBytes());
                //Assert that the contents are also the same.
                rawByteBuf.resetReaderIndex();
                assertEquals(0, ByteBufUtil.compare(rawByteBuf, newRawByteBuf));

                final long newRawByteBufChecksum = ByteBufs.calculateChecksum(newRawByteBuf);
                final long slowNewRawByteBufChecksum = ByteBufs.calculateChecksumNio(newRawByteBuf);
                log.info("Checksum production: [{}], test: [{}]", newRawByteBufChecksum, slowNewRawByteBufChecksum);
                assertEquals(rawByteBufChecksum, newRawByteBufChecksum);
                assertEquals(rawByteBufChecksum, slowNewRawByteBufChecksum);
            } finally {
                newRawByteBuf.release();
            }
        } finally {
            rawByteBuf.release();
        }
    }
}
