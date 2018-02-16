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
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.nukleus.tcp.internal.types.stream.Flag.FIN;
import static org.reaktivity.nukleus.tcp.internal.types.stream.Flag.RST;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.ListFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TransferFW;

public final class WriteStream
{
    // Mina uses a value of 256 (see AbstractPollingIoProcessor.writeBuffer).
    // Netty uses a configurable value, defaulting to 16
    // (see https://netty.io/4.0/api/io/netty/channel/ChannelConfig.html#setWriteSpinCount(int))
    public static final int WRITE_SPIN_COUNT = 16;

    private final long throttleId;
    private final MessageConsumer throttle;
    private final SocketChannel channel;
    private final Poller poller;

    private final ByteBuffer writeBuffer;
    private final StreamHelper helper;
    private final ToIntFunction<PollerKey> writeHandler;

    private final LongSupplier countOverflows;
    private final LongSupplier countFrames;
    private final LongConsumer countBytes;

    private PollerKey key;
    private MessageConsumer correlatedInput;
    private long correlatedStreamId;

    private int flags;
    private long backlogAddress;
    private int writeOffset;

    WriteStream(
        MessageConsumer throttle,
        long throttleId,
        SocketChannel channel,
        Poller poller,
        ByteBuffer writeBuffer,
        StreamHelper writer,
        LongSupplier countOverflows,
        LongSupplier countFrames,
        LongConsumer countBytes)
    {
        this.throttleId = throttleId;
        this.throttle = throttle;
        this.channel = channel;
        this.poller = poller;
        this.writeBuffer = writeBuffer;
        this.helper = writer;
        this.countOverflows = countOverflows;
        this.countFrames = countFrames;
        this.countBytes = countBytes;
        this.writeHandler = this::onNotifyWritable;
        this.backlogAddress = -1L;
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
            final BeginFW begin = helper.beginRO.wrap(buffer, index, index + length);
            onBegin(begin);
            break;
        case TransferFW.TYPE_ID:
            final TransferFW transfer = helper.transferRO.wrap(buffer, index, index + length);
            onTransfer(transfer);
            break;
        default:
            // ignore
            break;
        }
    }

    void onConnected()
    {
        int flags = 0;

        try
        {
            this.key = this.poller.doRegister(channel, OP_WRITE, writeHandler);
        }
        catch (IOException ex)
        {
            flags = RST.set(flags);
        }

        helper.doAck(throttle, throttleId, flags);
    }

    void onConnectFailed()
    {
        helper.doAck(throttle, throttleId, RST.flag());
    }

    void setCorrelatedInput(
        long correlatedStreamId,
        MessageConsumer correlatedInput)
    {
        this.correlatedInput = correlatedInput;
        this.correlatedStreamId = correlatedStreamId;
    }

    private void onBegin(
        BeginFW begin)
    {
        // nop, wait for either onConnected() or onConnectFailed()
    }

    private void onTransfer(
        TransferFW transfer)
    {
        countFrames.getAsLong();

        final int flags = transfer.flags();
        ListFW<RegionFW> regions = helper.appendBacklogRegions(backlogAddress, transfer.regions());

        if (RST.check(flags))
        {
            doShutdownOutput();

            helper.doAck(throttle, throttleId, flags, regions);
            backlogAddress = helper.releaseWriteMemory(backlogAddress);
        }
        else
        {
            try
            {
                final DirectBuffer buffer = helper.toDirectBuffer(regions, writeOffset);
                final int remainingBytes = buffer.capacity();
                final int writableBytes = Math.min(remainingBytes, writeBuffer.capacity());

                int bytesWritten = 0;

                if (writableBytes > 0)
                {
                    writeBuffer.clear();
                    buffer.getBytes(0, writeBuffer, writableBytes);
                    writeBuffer.flip();

                    for (int i = WRITE_SPIN_COUNT; bytesWritten == 0 && i > 0; i--)
                    {
                        bytesWritten = channel.write(writeBuffer);
                    }

                    countBytes.accept(bytesWritten);
                }

                if (bytesWritten < remainingBytes)
                {
                    final long backlogAddress = helper.acquireWriteMemory(this.backlogAddress);

                    if (backlogAddress == -1L)
                    {
                        countOverflows.getAsLong();
                        helper.doAck(throttle, throttleId, RST.flag(), regions);

                        doShutdownOutput();
                    }
                    else
                    {
                        if (this.backlogAddress == -1L)
                        {
                            helper.setBacklogRegions(backlogAddress, regions);
                            this.backlogAddress = backlogAddress;
                        }

                        writeOffset += bytesWritten;
                        this.flags |= flags;

                        key.register(OP_WRITE);
                    }
                }
                else
                {
                    if (FIN.check(flags))
                    {
                        doShutdownOutput();
                    }
                    helper.doAck(throttle, throttleId, flags, regions);
                    backlogAddress = helper.releaseWriteMemory(backlogAddress);
                    writeOffset = 0;
                }
            }
            catch (IOException ex)
            {
                onWriteFailed(ex, regions);
            }
        }
    }

    private int onNotifyWritable(
        PollerKey key)
    {
        final ListFW<RegionFW> regions = helper.wrapBacklogRegions(backlogAddress);

        key.clear(OP_WRITE);

        int bytesWritten = 0;
        try
        {
            final DirectBuffer buffer = helper.toDirectBuffer(regions, writeOffset);
            final int writableBytes = Math.min(buffer.capacity(), writeBuffer.capacity());

            writeBuffer.clear();
            buffer.getBytes(0, writeBuffer, writableBytes);
            writeBuffer.flip();
            bytesWritten = channel.write(writeBuffer);

            if (bytesWritten < writableBytes)
            {
                writeOffset += bytesWritten;
                key.register(OP_WRITE);
            }
            else
            {
                if (FIN.check(flags))
                {
                    doShutdownOutput();
                }

                helper.doAck(throttle, throttleId, flags, regions);

                backlogAddress = helper.releaseWriteMemory(backlogAddress);
            }
        }
        catch (IOException ex)
        {
            onWriteFailed(ex, regions);
        }

        return bytesWritten;
    }

    private void onWriteFailed(
        IOException ex,
        ListFW<RegionFW> regions)
    {
        // IOException from write implies channel input and output will no longer function
        if (correlatedInput != null)
        {
            helper.doTcpTransfer(correlatedInput, correlatedStreamId, RST.flag());
        }

        helper.doAck(throttle, throttleId, RST.flag(), regions);

        backlogAddress = helper.releaseWriteMemory(backlogAddress);

        doShutdownOutput();
    }

    private void doShutdownOutput()
    {
        if (channel.isConnected())
        {
            key.clear(OP_WRITE);
            try
            {
                channel.shutdownOutput();
            }
            catch (IOException ex)
            {
                rethrowUnchecked(ex);
            }
        }
    }
}
