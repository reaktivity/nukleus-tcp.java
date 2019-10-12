/**
 * Copyright 2016-2019 The Reaktivity Project
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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SocketChannel;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;
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

    private final long routeId;
    private final long streamId;
    private final MessageConsumer sourceThrottle;
    private final SocketChannel channel;
    private final Poller poller;
    private final BufferPool bufferPool;

    private final MessageWriter writer;
    private final ToIntFunction<PollerKey> writeHandler;

    private final TcpRouteCounters counters;
    private final Runnable onConnectionClosed;

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
        long routeId,
        long streamId,
        SocketChannel channel,
        Poller poller,
        BufferPool bufferPool,
        ByteBuffer writeBuffer,
        MessageWriter writer,
        TcpRouteCounters counters,
        int windowThreshold,
        Runnable onConnectionClosed)
    {
        this.routeId = routeId;
        this.streamId = streamId;
        this.sourceThrottle = sourceThrottle;
        this.channel = channel;
        this.poller = poller;
        this.bufferPool = bufferPool;
        this.writeBuffer = writeBuffer;
        this.writer = writer;
        this.writeHandler = this::handleWrite;
        this.counters = counters;
        this.windowThreshold = windowThreshold;
        this.onConnectionClosed = onConnectionClosed;
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = writer.beginRO.wrap(buffer, index, index + length);
            onBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = writer.dataRO.wrap(buffer, index, index + length);
            onData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = writer.endRO.wrap(buffer, index, index + length);
            onEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = writer.abortRO.wrap(buffer, index, index + length);
            onAbort(abort);
            break;
        default:
            // ignore
            break;
        }
    }

    void onConnected()
    {
        if (isInitial(streamId))
        {
            counters.opensRead.getAsLong();
        }

        this.key = this.poller.doRegister(channel, 0, null);
        this.key.handler(OP_WRITE, writeHandler);
        offerWindow(bufferPool.slotCapacity());
    }

    void onConnectFailed()
    {
        if (channel.isOpen())
        {
            CloseHelper.quietClose(channel);
            onConnectionClosed.run();
        }

        if (isInitial(streamId))
        {
            counters.resetsRead.getAsLong();
        }

        writer.doReset(sourceThrottle, routeId, streamId);
    }

    void setCorrelatedInput(
        long correlatedStreamId,
        MessageConsumer correlatedInput)
    {
        this.correlatedInput = correlatedInput;
        this.correlatedStreamId = correlatedStreamId;
    }

    private void handleIOExceptionFromWrite()
    {
        if (isInitial(streamId))
        {
            counters.resetsRead.getAsLong();
        }

        // IOException from write implies channel input and output will no longer function
        if (correlatedInput != null)
        {
            writer.doTcpAbort(correlatedInput, routeId, correlatedStreamId);
        }

        if (isInitial(streamId))
        {
            counters.abortsWritten.getAsLong();
        }

        CloseHelper.quietClose(channel::shutdownInput);

        doFail();
    }

    private void onAbort(
        AbortFW abort)
    {
        if (isInitial(streamId))
        {
            counters.abortsWritten.getAsLong();
        }

        if (slot != NO_SLOT) // partial writes pending
        {
            bufferPool.release(slot);
        }
        doCleanup();
    }

    private void onBegin(
        BeginFW begin)
    {
        // No-op - doConnected() should be called instead once the connection has been established
    }

    private void onData(
        DataFW data)
    {
        try
        {
            final OctetsFW payload = data.payload();
            final int writableBytes = data.length();

            if (reduceWindow(writableBytes))
            {
                final ByteBuffer writeBuffer = toWriteBuffer(payload.buffer(), payload.offset(), writableBytes);
                final int remainingBytes = writeBuffer.remaining();

                int bytesWritten = 0;

                for (int i = WRITE_SPIN_COUNT; bytesWritten == 0 && i > 0; i--)
                {
                    bytesWritten = channel.write(writeBuffer);
                }

                if (isInitial(streamId))
                {
                    counters.bytesWritten.accept(bytesWritten);
                }

                int originalSlot = slot;
                if (handleUnwrittenData(writeBuffer, bytesWritten))
                {
                    if (bytesWritten < remainingBytes)
                    {
                        key.register(OP_WRITE);
                        counters.writeops.getAsLong();
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
                    writer.doReset(sourceThrottle, routeId, streamId);
                }
            }
        }
        catch (IOException ex)
        {
            handleIOExceptionFromWrite();
        }
    }

    private void onEnd(
        EndFW end)
    {
        if (slot == NO_SLOT) // no partial writes pending
        {
            if (isInitial(streamId))
            {
                counters.closesWritten.getAsLong();
            }
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
        writer.doReset(sourceThrottle, routeId, streamId);
        if (slot != NO_SLOT)
        {
            bufferPool.release(slot);
        }
        doCleanup();
    }

    private void doCleanup()
    {
        if (key != null && key.isValid())
        {
            key.clear(OP_WRITE);
        }

        if (!channel.isConnectionPending())
        {
            CloseHelper.quietClose(channel::shutdownOutput);
        }
        closeIfInputShutdown();
    }

    private ByteBuffer toWriteBuffer(
        DirectBuffer data,
        int dataOffset,
        int dataLength)
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

    private boolean handleUnwrittenData(
        ByteBuffer written,
        int bytesWritten)
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
                    counters.overflows.getAsLong();
                    doFail();
                    result = false;
                }
                else
                {
                    counters.partials.getAsLong();

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
        int bytesWritten = 0;

        try
        {
            key.clear(OP_WRITE);
            ByteBuffer writeBuffer = bufferPool.byteBuffer(slot);
            writeBuffer.position(slotOffset);
            writeBuffer.limit(slotPosition);

            bytesWritten = channel.write(writeBuffer);

            if (isInitial(streamId))
            {
                counters.bytesWritten.accept(bytesWritten);
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
                counters.writeops.getAsLong();
            }
        }
        catch (IOException | CancelledKeyException ex)
        {
            handleIOExceptionFromWrite();
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
            writer.doWindow(sourceThrottle, routeId, streamId, 0, pendingCredit, 0);
            pendingCredit = 0;
        }
    }

    private void closeIfInputShutdown()
    {
        if (channel.socket().isInputShutdown())
        {
            if (channel.isOpen())
            {
                CloseHelper.quietClose(channel);
                onConnectionClosed.run();
            }
        }
    }

    private static boolean isInitial(
        long streamId)
    {
        return (streamId & 0x0000_0000_0000_0001L) != 0L;
    }
}
