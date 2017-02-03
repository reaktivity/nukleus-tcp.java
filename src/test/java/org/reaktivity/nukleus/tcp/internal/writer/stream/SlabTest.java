/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.tcp.internal.writer.stream;

import static org.agrona.BitUtil.findNextPositivePowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Before;
import org.junit.Test;

public class SlabTest
{

    private DirectBuffer data;

    @Before
    public void before() throws Exception
    {
        ExpandableArrayBuffer data = new ExpandableArrayBuffer(100);
        data.putStringWithoutLengthUtf8(0, "test data");
        this.data = data;
    }

    @Test
    public void getShouldReturnDefaultWriteBufferForNoSlotCase()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 6);
        assertEquals(6, buffer.remaining());
        assertEquals(128, buffer.capacity());
        assertTrue(buffer.isDirect());
        assertEquals(ByteBuffer.wrap("test d".getBytes()), buffer);

        ByteBuffer buffer2 = slab.get(slot, data, 2, 5);
        assertSame(buffer, buffer2);
        assertEquals(5, buffer.remaining());
        assertEquals(ByteBuffer.wrap("st da".getBytes()), buffer);
    }

    @Test
    public void getShouldReturnUnwrittenData()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 9);
        buffer.get();
        buffer.get();
        slot = slab.written(111, slot, buffer);
        assertTrue(slot >= 0);

        ByteBuffer buffer2 = slab.get(slot);
        assertNotSame(buffer, buffer2);
        assertEquals(ByteBuffer.wrap("st data".getBytes()), buffer2);
    }

    @Test
    public void getShouldReturnUnwrittenDataPlusNewData()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer);
        assertTrue(slot >= 0);

        ByteBuffer buffer2 = slab.get(slot, data, 5, 4);
        assertEquals(ByteBuffer.wrap("test data".getBytes()), buffer2);
    }

    @Test
    public void getShouldReturnPartiallyUnwrittenDataPlusNewData()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        buffer.get();
        buffer.get();
        slot = slab.written(111, slot, buffer);
        assertTrue(slot >= 0);

        ByteBuffer buffer2 = slab.get(slot, data, 5, 4);
        assertEquals(ByteBuffer.wrap("st data".getBytes()), buffer2);
    }

    @Test
    public void writtenShouldAllocateDifferentSlotsForDifferentStreams() throws Exception
    {
        int slot1 = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot1, data, 0, 5);
        slot1 = slab.written(111, slot1, buffer);
        assertTrue(slot1 >= 0);

        int slot2 = Slab.NO_SLOT;
        ByteBuffer buffer2 = slab.get(slot2, data, 2, 7);
        slot2 = slab.written(111, slot2, buffer2);
        assertNotEquals(slot1, slot2);

        assertEquals(ByteBuffer.wrap("test ".getBytes()), slab.get(slot1));
        assertEquals(ByteBuffer.wrap("st da".getBytes()), slab.get(slot2));
    }

    @Test
    public void writtenShouldReportOutOfMemory() throws Exception
    {
        Slab slab = new Slab(10, 120);
        int slot = 0;
        for (int i=0; i < findNextPositivePowerOfTwo(10); i++)
        {
            slot = Slab.NO_SLOT;
            ByteBuffer buffer = slab.get(slot, data, 0, 5);
            slot = slab.written(111, slot, buffer);
            assertTrue(slot >= 0);
        }
        slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer);
        assertEquals(Slab.OUT_OF_MEMORY, slot);
    }

}
