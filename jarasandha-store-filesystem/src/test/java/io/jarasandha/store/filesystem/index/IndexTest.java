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
package io.jarasandha.store.filesystem.index;

import io.jarasandha.store.api.Block;
import io.jarasandha.store.api.Blocks;
import io.jarasandha.store.api.LogicalRecordLocation;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;

/**
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public class IndexTest {
    @Test
    public void testUsingRecordPositions() {
        final Block block1 = new SimpleBlock(0, 1000, 3004);
        final Block block2 = new SimpleBlock(0, 1000, 3 /*3007*/);
        final Block block3 = new SimpleBlock(0, 1000, 102 /*3109*/);
        final Block block4 = new SimpleBlock(0, 1000, 440 /*3549*/);
        final Block block5 = new SimpleBlock(0, 1000, 8991 /*12540*/);
        final Block block6 = new SimpleBlock(0, 1000, 0);
        final Block block7 = new SimpleBlock(0, 1000, 1 /*12541*/);
        final SimpleIndex<Block> index =
                new SimpleIndex<>(newArrayList(block1, block2, block3, block4, block5, block6, block7));

        final Blocks<Block> blocks = new Blocks<>(index);

        //One extreme.
        assertNull(blocks.findBlockRefForRecordPosition(-1));

        assertEquals(0, blocks.findBlockRefForRecordPosition(0).getBlockPosition());
        assertSame(block1, blocks.findBlockRefForRecordPosition(0).getBlock());
        assertEquals(0, blocks.findBlockRefForRecordPosition(1).getBlockPosition());
        assertSame(block1, blocks.findBlockRefForRecordPosition(1).getBlock());
        assertEquals(0, blocks.findBlockRefForRecordPosition(100).getBlockPosition());
        assertSame(block1, blocks.findBlockRefForRecordPosition(100).getBlock());
        assertEquals(0, blocks.findBlockRefForRecordPosition(3003).getBlockPosition());
        assertSame(block1, blocks.findBlockRefForRecordPosition(3003).getBlock());
        assertEquals(0, blocks.findBlockRefForRecordPosition(block1.numRecords() - 1).getBlockPosition());
        assertSame(block1, blocks.findBlockRefForRecordPosition(block1.numRecords() - 1).getBlock());

        assertEquals(1, blocks.findBlockRefForRecordPosition(block1.numRecords()).getBlockPosition());
        assertSame(block2, blocks.findBlockRefForRecordPosition(block1.numRecords()).getBlock());
        assertEquals(2, blocks.findBlockRefForRecordPosition(3108).getBlockPosition());
        assertSame(block3, blocks.findBlockRefForRecordPosition(3108).getBlock());
        assertEquals(3, blocks.findBlockRefForRecordPosition(3458).getBlockPosition());
        assertSame(block4, blocks.findBlockRefForRecordPosition(3458).getBlock());
        assertEquals(4, blocks.findBlockRefForRecordPosition(12538).getBlockPosition());
        assertSame(block5, blocks.findBlockRefForRecordPosition(12538).getBlock());
        assertSame(block5, blocks.findBlockRefForRecordPosition(12539).getBlock());
        assertEquals(6, blocks.findBlockRefForRecordPosition(12540).getBlockPosition());
        assertSame(block7, blocks.findBlockRefForRecordPosition(12540).getBlock());

        //Other extreme.
        final int totalRecords = index
                .blocks()
                .stream()
                .mapToInt(Block::numRecords)
                .sum();
        assertNull(blocks.findBlockRefForRecordPosition(totalRecords));
        assertNull(blocks.findBlockRefForRecordPosition(totalRecords + 1));
        assertNull(blocks.findBlockRefForRecordPosition(Integer.MAX_VALUE));

        //Check the logical mappings now.

        LogicalRecordLocation llr = blocks.localize(0);
        assertEquals(0, llr.getPositionOfBlock());
        assertEquals(0, llr.getPositionOfRecordInBlock());

        llr = blocks.localize(3003);
        assertEquals(0, llr.getPositionOfBlock());
        assertEquals(3003, llr.getPositionOfRecordInBlock());

        llr = blocks.localize(3003);
        assertEquals(0, llr.getPositionOfBlock());
        assertEquals(3003, llr.getPositionOfRecordInBlock());

        llr = blocks.localize(12540);
        assertEquals(6 /*Not 5 as that had 0 entries*/, llr.getPositionOfBlock());
        assertEquals(0, llr.getPositionOfRecordInBlock());
    }
}
