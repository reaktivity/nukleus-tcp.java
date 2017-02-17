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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.IntConsumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Hashing;

/**
 * A chunk of shared memory for storing unwritten data from partial writes. Methods are provided for
 * getting a write buffer and storing remaining data in the case of a partial write. The memory
 * is segmented into slots, the size of each lot being the maximum window size (maximum possible
 * amount of unwritten data when a socket becomes unwritable). A slot is acquired when data cannot
 * be written for a stream and is used to store the pending data until it is all successfully written.
 * <b>Each instance of this class is assumed to be used by one and only one thread.</b>
 */
class Slab
{
    static final int OUT_OF_MEMORY = -2;
    static final int NO_SLOT = -1;

    private static final int UNUSED = -1;
    private final int slotSize;
    private final int slotSizeBits;
    private final ByteBuffer buffer;
    private final ByteBuffer writeBuffer;
    private final int offsets[]; // starting position for slot data in buffer
    private final int remaining[]; // length of data in slot
    private int acquiredSlots = 0;

    Slab(int maximumPartiallyWrittenStreams, int maximumWindowSize)
    {
        validateGT0(maximumPartiallyWrittenStreams, "maximumPartiallyWrittenStreams");
        validateGT0(maximumWindowSize, "maximumWindowSize");
        int slots = findNextPositivePowerOfTwo(maximumPartiallyWrittenStreams);
        slotSize = findNextPositivePowerOfTwo(maximumWindowSize);
        slotSizeBits = Integer.numberOfTrailingZeros(slotSize);
        int capacityInBytes = slots << slotSizeBits;
        buffer = ByteBuffer.allocateDirect(capacityInBytes);
        writeBuffer = ByteBuffer.allocateDirect(slotSize);
        offsets = new int[slots];
        Arrays.fill(offsets, UNUSED);
        remaining = new int[slots];
    }

    public ByteBuffer get(int slot, DirectBuffer data, int offset, int length)
    {
        ByteBuffer result;
        if (slot == NO_SLOT)
        {
            writeBuffer.clear();
            data.getBytes(offset, writeBuffer, length);
            writeBuffer.flip();
            result = writeBuffer;
        }
        else
        {
            // Append the data to the previous remaining data
            buffer.position(offsets[slot] + remaining[slot]);
            buffer.limit((slot << slotSizeBits) + slotSize);
            data.getBytes(offset, buffer, length);
            remaining[slot] += length;
            buffer.position(offsets[slot]);
            buffer.limit(buffer.position() + remaining[slot]);
            result = buffer;
        }
        return result;
    }

    public ByteBuffer get(int slot)
    {
        ByteBuffer result = null;
        if (slot != NO_SLOT)
        {
            buffer.limit(offsets[slot] + remaining[slot]);
            buffer.position(offsets[slot]);
            result = buffer;
        }
        return result;
    }

    public int written(long streamId, int slot, ByteBuffer written, int bytesWritten, IntConsumer offerWindow)
    {
        int nextSlot = slot;
        if (slot == NO_SLOT)
        {
            if (written.hasRemaining())
            {
                // store the remaining data into a new slot
                nextSlot = acquire(streamId);
                if (nextSlot != OUT_OF_MEMORY)
                {
                    remaining[nextSlot] = written.remaining();
                    buffer.limit(offsets[nextSlot] + slotSize);
                    buffer.position(offsets[nextSlot]);
                    buffer.put(written);
                    buffer.limit(buffer.position());
                    buffer.position(offsets[nextSlot]);
                    if (bytesWritten > 0)
                    {
                        offerWindow.accept(bytesWritten);
                    }
                }
            }
            else if (bytesWritten > 0)
            {
                offerWindow.accept(bytesWritten);
            }
        }
        else
        {
            if (written.hasRemaining())
            {
                // Some data from the existing slot was written, adjust offset and remaining
                offsets[slot] = written.position();
                remaining[slot] = written.remaining();
            }
            else
            {
                // Free the slot, but first send a window update for all data that had ever been saved in the slot
                int slotStart = (slot << slotSizeBits);
                offerWindow.accept(written.position() - slotStart);
                release(slot);
                nextSlot = NO_SLOT;
            }
        }
        return nextSlot;
    }

    int acquire(long streamId)
    {
        if (acquiredSlots == offsets.length)
        {
            return OUT_OF_MEMORY;
        }
        final int mask = offsets.length - 1;
        int slot = Hashing.hash(streamId, mask);
        while (offsets[slot] != UNUSED)
        {
            slot = ++slot & mask;
        }
        offsets[slot] = slot << slotSizeBits;
        acquiredSlots++;
        return slot;
    }

    void release(int slot)
    {
        offsets[slot] = UNUSED;
        remaining[slot] = 0;
        acquiredSlots--;
    }

    private void validateGT0(int value, String argumentName)
    {
        if (value <= 0)
        {
            throw new IllegalArgumentException(argumentName + " must be > 0");
        }
    }

}
