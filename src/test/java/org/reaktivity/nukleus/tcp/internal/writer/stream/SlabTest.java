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
import java.util.function.IntConsumer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SlabTest
{
    private static final String TEST_DATA = "test data";

    private DirectBuffer data;
    private IntConsumer windowUpdater;

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
        {
            {
                windowUpdater = mock(IntConsumer.class, "offerWindow");
            }
        };

    @Before
    public void before() throws Exception
    {
        ExpandableArrayBuffer data = new ExpandableArrayBuffer(100);
        data.putStringWithoutLengthUtf8(0, TEST_DATA);
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
    public void getShouldReturnUnwrittenDataWrittenShouldUpdateWindowForFirstWrittenData()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 9);
        buffer.get();
        buffer.get();
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(2);
            }
        });
        slot = slab.written(111, slot, buffer, 2, windowUpdater);
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
        slot = slab.written(111, slot, buffer, 0, windowUpdater);
        assertTrue(slot >= 0);

        ByteBuffer buffer2 = slab.get(slot, data, 5, 4);
        assertEquals(ByteBuffer.wrap(TEST_DATA.getBytes()), buffer2);
    }

    @Test
    public void getShouldReturnPartiallyUnwrittenDataPlusNewData()
    {
        int slot = Slab.NO_SLOT;
        Slab slab = new Slab(10, 120);
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        buffer.get();
        buffer.get();
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(2);
            }
        });
        slot = slab.written(111, slot, buffer, 2, windowUpdater);
        assertTrue(slot >= 0);

        ByteBuffer buffer2 = slab.get(slot, data, 5, 4);
        assertEquals(ByteBuffer.wrap("st data".getBytes()), buffer2);
    }

    @Test
    public void writtenShouldNotUpdateWindowWhenNoDataWasWrittenNewSlot()
    {
        Slab slab = new Slab(10, 120);
        int slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer, 0, windowUpdater);
        assertTrue(slot >= 0);
    }

    @Test
    public void writtenShouldNotUpdateWindowWhenNoDataWasWrittenExistingSlot()
    {
        Slab slab = new Slab(10, 120);
        int slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer, 0, windowUpdater);

        buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer, 0, windowUpdater);
    }

    @Test
    public void writtenShouldNotUpdateWindowWhenNotAllDataWasWrittenExistingSlot()
    {
        Slab slab = new Slab(10, 120);
        int slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        buffer.get();
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(1);
            }
        });
        slot = slab.written(111, slot, buffer, 1, windowUpdater);
        assertTrue(slot >= 0);

        buffer = slab.get(slot);
        buffer.position(buffer.limit() - 1);
        final int written = TEST_DATA.length() - 2;
        slot = slab.written(111, slot, buffer, written, windowUpdater);
    }

    @Test
    public void writtenShouldUpdateWindowWhenAllDataWasWrittenExistingSlot()
    {
        Slab slab = new Slab(10, 120);
        int slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, TEST_DATA.length());
        int written = 1;
        buffer.position(buffer.position() + written);
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(1);
            }
        });
        slot = slab.written(111, slot, buffer, written, windowUpdater);
        assertTrue(slot >= 0);

        buffer = slab.get(slot);
        written = TEST_DATA.length() - 1;
        buffer.position(buffer.position() + written);
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(TEST_DATA.length() - 1);
            }
        });
        slot = slab.written(111, slot, buffer, written, windowUpdater);
    }

    @Test
    public void writtenShouldUpdateWindowWhenAllDataWasWrittenInPiecesExistingSlot()
    {
        Slab slab = new Slab(10, 120);
        int slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, TEST_DATA.length());
        int written = 1;
        buffer.position(buffer.position() + written);
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(1);
            }
        });
        slot = slab.written(111, slot, buffer, written, windowUpdater);
        assertTrue(slot >= 0);

        buffer = slab.get(slot);
        written = 3;
        buffer.position(buffer.position() + written);
        slot = slab.written(111, slot, buffer, written, windowUpdater);

        buffer = slab.get(slot);
        written = TEST_DATA.length() - 4;
        buffer.position(buffer.position() + written);
        context.checking(new Expectations()
        {
            {
                oneOf(windowUpdater).accept(TEST_DATA.length() - 1);
            }
        });
        slot = slab.written(111, slot, buffer, written, windowUpdater);
    }

    @Test
    public void writtenShouldAllocateDifferentSlotsForDifferentStreams() throws Exception
    {
        Slab slab = new Slab(10, 120);

        int slot1 = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot1, data, 0, 5);
        slot1 = slab.written(111, slot1, buffer, 0, windowUpdater);
        assertTrue(slot1 >= 0);

        int slot2 = Slab.NO_SLOT;
        ByteBuffer buffer2 = slab.get(slot2, data, 2, 7);
        slot2 = slab.written(112, slot2, buffer2, 0, windowUpdater);
        assertNotEquals(slot1, slot2);

        assertEquals(ByteBuffer.wrap("test ".getBytes()), slab.get(slot1));
        assertEquals(ByteBuffer.wrap("st data".getBytes()), slab.get(slot2));
    }

    @Test
    public void writtenShouldAllocateDifferentSlotsForDifferentStreamsWithSameHashcode() throws Exception
    {
        Slab slab = new Slab(10, 120);

        int slot1 = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot1, data, 0, 5);
        slot1 = slab.written(1, slot1, buffer, 0, windowUpdater);
        assertTrue(slot1 >= 0);

        int slot2 = Slab.NO_SLOT;
        ByteBuffer buffer2 = slab.get(slot2, data, 2, 7);
        slot2 = slab.written(17, slot2, buffer2, 0, windowUpdater);
        assertNotEquals(slot1, slot2);

        assertEquals(ByteBuffer.wrap("test ".getBytes()), slab.get(slot1));
        assertEquals(ByteBuffer.wrap("st data".getBytes()), slab.get(slot2));
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
            int streamId = 111 + i;
            slot = slab.written(streamId, slot, buffer, 0, windowUpdater);
            assertTrue(slot >= 0);
        }
        slot = Slab.NO_SLOT;
        ByteBuffer buffer = slab.get(slot, data, 0, 5);
        slot = slab.written(111, slot, buffer, 0, windowUpdater);
        assertEquals(Slab.OUT_OF_MEMORY, slot);
    }

}
