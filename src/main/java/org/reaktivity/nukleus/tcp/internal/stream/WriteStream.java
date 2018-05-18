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
package org.reaktivity.nukleus.tcp.internal.stream;

import static java.nio.channels.SelectionKey.OP_WRITE;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;

public final class WriteStream
{
    // Mina uses a value of 256 (see AbstractPollingIoProcessor.writeBuffer).
    // Netty uses a configurable value, defaulting to 16
    // (see https://netty.io/4.0/api/io/netty/channel/ChannelConfig.html#setWriteSpinCount(int))
    public static final int WRITE_SPIN_COUNT = 16;

    private static final int EOS_REQUESTED = -1;

    private final long streamId;
    private final MessageConsumer sourceThrottle;
    private final SocketChannel channel;
    private final Poller poller;
    private final BufferPool bufferPool;

    private final MessageWriter writer;
    private final LongSupplier incrementOverflow;
    private final ToIntFunction<PollerKey> writeHandler;

    private final LongSupplier frameCounter;
    private final LongConsumer bytesAccumulator;

    private int slot = BufferPool.NO_SLOT;
    private int slotOffset; // index of the first byte of unwritten data
    private int slotPosition; // index of the byte following the last byte of unwritten data

    private PollerKey key;
    private int readableBytes;

    private ByteBuffer writeBuffer;

    private MessageConsumer correlatedInput;

    private long correlatedStreamId;

    private int windowThreshold;

    private int pendingCredit;

    WriteStream(
        MessageConsumer sourceThrottle,
        long streamId,
        SocketChannel channel,
        Poller poller,
        LongSupplier incrementOverflow,
        BufferPool bufferPool,
        ByteBuffer writeBuffer,
        MessageWriter writer,
        LongSupplier frameCounter,
        LongConsumer bytesAccumulator,
        int windowThreshold)
    {
        this.streamId = streamId;
        this.sourceThrottle = sourceThrottle;
        this.channel = channel;
        this.poller = poller;
        this.incrementOverflow = incrementOverflow;
        this.bufferPool = bufferPool;
        this.writeBuffer = writeBuffer;
        this.writer = writer;
        this.writeHandler = this::handleWrite;
        this.frameCounter = frameCounter;
        this.bytesAccumulator = bytesAccumulator;
        this.windowThreshold = windowThreshold;
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case AbortFW.TYPE_ID:
            processAbort(buffer, index, index + length);
            break;
        case BeginFW.TYPE_ID:
            processBegin(buffer, index, index + length);
            break;
        case DataFW.TYPE_ID:
            try
            {
                processData(buffer, index, index + length);
            }
            catch (IOException ex)
            {
                handleIOExceptionFromWrite();
            }
            break;
        case EndFW.TYPE_ID:
            processEnd(buffer, index, index + length);
            break;
        default:
            // ignore
            break;
        }
    }

    void doConnected()
    {
        this.key = this.poller.doRegister(channel, OP_WRITE, writeHandler);
        offerWindow(bufferPool.slotCapacity());
    }

    void doConnectFailed()
    {
        writer.doReset(sourceThrottle, streamId);
    }

    void setCorrelatedInput(long correlatedStreamId, MessageConsumer correlatedInput)
    {
        this.correlatedInput = correlatedInput;
        this.correlatedStreamId = correlatedStreamId;
    }

    private void handleIOExceptionFromWrite()
    {
        // IOEXception from write implies channel input and output will no longer function
        if (correlatedInput != null)
        {
            writer.doTcpAbort(correlatedInput, correlatedStreamId);
        }
        doFail();
    }

    private void processAbort(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        if (slot != NO_SLOT) // partial writes pending
        {
            bufferPool.release(slot);
        }
        doCleanup();
    }

    private void processBegin(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        // No-op - doConnected() should be called instead once the connection has been established
    }

    private void processData(
        DirectBuffer buffer,
        int offset,
        int limit) throws IOException
    {
        writer.dataRO.wrap(buffer, offset, limit);
        assert writer.dataRO.padding() == 0;

        final OctetsFW payload = writer.dataRO.payload();
        final int writableBytes = writer.dataRO.length();

        frameCounter.getAsLong();
        bytesAccumulator.accept(writableBytes);

        if (reduceWindow(writableBytes))
        {
            final ByteBuffer writeBuffer = getWriteBuffer(buffer, payload.offset(), writableBytes);
            final int remainingBytes = writeBuffer.remaining();

            int bytesWritten = 0;

            for (int i = WRITE_SPIN_COUNT; bytesWritten == 0 && i > 0; i--)
            {
                bytesWritten = channel.write(writeBuffer);
            }

            int originalSlot = slot;
            if (handleUnwrittenData(writeBuffer, bytesWritten))
            {
                if (bytesWritten < remainingBytes)
                {
                    key.register(OP_WRITE);
                }
                else if (originalSlot != NO_SLOT)
                {
                    // we just flushed out a pending write
                    key.clear(OP_WRITE);
                }
            }
        }
        else
        {
            if (slot == NO_SLOT)
            {
                doFail();
            }
            else
            {
                // send reset but defer cleanup until pending writes are completed
                writer.doReset(sourceThrottle, streamId);
            }
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        if (slot == NO_SLOT) // no partial writes pending
        {
            writer.endRO.wrap(buffer, offset, limit);
            doCleanup();
        }
        else
        {
            // Signal end of stream requested and ensure further data streams will result in reset
            readableBytes = EOS_REQUESTED;
        }
    }

    private void doFail()
    {
        writer.doReset(sourceThrottle, streamId);
        if (slot != NO_SLOT)
        {
            bufferPool.release(slot);
        }
        doCleanup();
    }

    private void doCleanup()
    {
        if (key != null)
        {
            key.clear(OP_WRITE);
        }

        if (channel.isConnected())
        {
            try
            {
                channel.shutdownOutput();
                closeIfInputShutdown();
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    private ByteBuffer getWriteBuffer(DirectBuffer data, int dataOffset, int dataLength)
    {
        ByteBuffer result;
        if (slot == NO_SLOT)
        {
            writeBuffer.clear();
            data.getBytes(dataOffset, writeBuffer, dataLength);
            writeBuffer.flip();
            result = writeBuffer;
        }
        else
        {
            // Append the data to the previous remaining data
            ByteBuffer buffer = bufferPool.byteBuffer(slot);
            buffer.position(slotPosition);
            data.getBytes(dataOffset, buffer, dataLength);
            slotPosition += dataLength;
            buffer.position(slotOffset);
            buffer.limit(slotPosition);
            result = buffer;
        }
        return result;
    }

    private boolean handleUnwrittenData(ByteBuffer written, int bytesWritten)
    {
        boolean result = true;
        if (slot == NO_SLOT)
        {
            if (written.hasRemaining())
            {
                // store the remaining data into a new slot
                slot = bufferPool.acquire(streamId);
                if (slot == NO_SLOT)
                {
                    incrementOverflow.getAsLong();
                    doFail();
                    result = false;
                }
                else
                {
                    ByteBuffer buffer = bufferPool.byteBuffer(slot);
                    slotOffset = buffer.position();
                    buffer.position(slotOffset);
                    buffer.put(written);
                    slotPosition = buffer.position();
                    if (bytesWritten > 0)
                    {
                        offerWindow(bytesWritten);
                    }
                }
            }
            else if (bytesWritten > 0)
            {
                offerWindow(bytesWritten);
            }
        }
        else
        {
            if (written.hasRemaining())
            {
                // Some data from the existing slot was written, adjust offset and remaining
                slotOffset = written.position();
            }
            else
            {
                // Free the slot, but first send a window update for all data that had ever been saved in the slot
                int slotStart = bufferPool.byteBuffer(slot).position();
                offerWindow(slotPosition - slotStart);
                bufferPool.release(slot);
                slot = NO_SLOT;
            }
        }
        return result;
    }

    private int handleWrite(
        PollerKey key)
    {
        key.clear(OP_WRITE);
        ByteBuffer writeBuffer = bufferPool.byteBuffer(slot);
        writeBuffer.position(slotOffset);
        writeBuffer.limit(slotPosition);

        int bytesWritten = 0;
        try
        {
            bytesWritten = channel.write(writeBuffer);
        }
        catch (IOException ex)
        {
            handleIOExceptionFromWrite();
            return 0;
        }

        handleUnwrittenData(writeBuffer, bytesWritten);

        if (slot == NO_SLOT)
        {
            if (readableBytes < 0) // deferred EOS and/or window was exceeded
            {
                doCleanup();
            }
        }
        else
        {
            // incomplete write
            key.register(OP_WRITE);
        }
        return bytesWritten;
    }

    private boolean reduceWindow(int update)
    {
        readableBytes -= update;
        return readableBytes >= 0;
    }

    private void offerWindow(final int credit)
    {
        pendingCredit += credit;

        // If readableBytes indicates EOS has been received we must not destroy that information
        // (and in this case there is no need to write the window update)
        // We can also get update < 0 if we received data GT window (protocol violation) while
        // we have data waiting to be written (incomplete writes)
        if (pendingCredit >= windowThreshold && readableBytes > EOS_REQUESTED)
        {
            readableBytes += pendingCredit;
            writer.doWindow(sourceThrottle, streamId, pendingCredit, 0, 0);
            pendingCredit = 0;
        }
    }

    private void closeIfInputShutdown()
    {
        if (channel.socket().isInputShutdown())
        {
            CloseHelper.quietClose(channel);
        }
    }

}
