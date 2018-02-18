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

import static java.nio.channels.SelectionKey.OP_READ;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.nukleus.tcp.internal.types.stream.Flag.FIN;
import static org.reaktivity.nukleus.tcp.internal.types.stream.Flag.RST;

import java.io.IOException;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.ListFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AckFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.RegionFW;

final class ReadStream
{
    private final MessageConsumer target;
    private final long targetId;
    private final PollerKey key;
    private final SocketChannel channel;
    private final StreamHelper helper;
    private final LongSupplier countFrames;
    private final LongConsumer countBytes;
    private final long memoryAddress;
    private final int readCapacityMask;

    private long ackIndex;
    private long ackIndexHighMark;
    private int ackIndexProgress;

    private long readIndex;

    private MessageConsumer correlatedThrottle;
    private long correlatedStreamId;
    private boolean resetRequired;

    ReadStream(
        MessageConsumer target,
        long targetId,
        PollerKey key,
        SocketChannel channel,
        StreamHelper helper,
        LongSupplier countFrames,
        LongConsumer countBytes)
    {
        this.target = target;
        this.targetId = targetId;
        this.key = key;
        this.channel = channel;
        this.helper = helper;
        this.countFrames = countFrames;
        this.countBytes = countBytes;
        this.memoryAddress = helper.acquireReadMemory();
        this.readCapacityMask = helper.readMemoryMask();
    }

    int onNotifyReadable(
        PollerKey key)
    {
        ByteBuffer readByteBuffer = helper.readByteBuffer((int)(readIndex - ackIndex));

        int bytesRead = 0;
        try
        {
            bytesRead = channel.read(readByteBuffer);
        }
        catch (IOException ex)
        {
            onReadException(ex);
        }

        if (bytesRead == -1)
        {
            onReadClosed();
        }
        else if (bytesRead != 0)
        {
            final int memoryReadOffset = helper.memoryOffset(readIndex);
            final int memoryReadLimit = memoryReadOffset + bytesRead;

            final MutableDirectBuffer memoryBuffer = helper.wrapMemory(memoryAddress);

            Consumer<ListFW.Builder<RegionFW.Builder, RegionFW>> regions;
            if (memoryReadLimit <= memoryBuffer.capacity())
            {
                int bytesRead0 = bytesRead;

                memoryBuffer.putBytes(memoryReadOffset, readByteBuffer, 0, bytesRead0);
                regions = rs -> rs.item(r -> r.address(memoryAddress + memoryReadOffset).length(bytesRead0).streamId(targetId));
            }
            else
            {
                int bytesRead0 = memoryBuffer.capacity() - memoryReadOffset;
                int bytesRead1 = bytesRead - bytesRead0;

                memoryBuffer.putBytes(memoryReadOffset, readByteBuffer, 0, bytesRead0);
                memoryBuffer.putBytes(0, readByteBuffer, bytesRead0, bytesRead1);
                regions = rs -> rs.item(r -> r.address(memoryAddress + memoryReadOffset).length(bytesRead0).streamId(targetId))
                                  .item(r -> r.address(memoryAddress).length(bytesRead1).streamId(targetId));
            }

            helper.doTcpTransfer(target, targetId, 0x00, regions);

            countFrames.getAsLong();
            countBytes.accept(bytesRead);

            readIndex += bytesRead;

            if (readByteBuffer.remaining() == 0)
            {
                key.clear(OP_READ);
            }
        }

        return 1;
    }

    private void onReadClosed()
    {
        // channel input closed
        ackIndex = -1;
        helper.doTcpTransfer(target, targetId, FIN.flag());
        key.cancel(OP_READ);
    }

    private void onReadException(
        IOException ex)
    {
        // TCP reset triggers IOException on read
        // implies channel input and output will no longer function
        ackIndex = -1;
        helper.doTcpTransfer(target, targetId, RST.flag());
        key.cancel(OP_READ);

        if (correlatedThrottle != null)
        {
            helper.doAck(correlatedThrottle, correlatedStreamId, RST.flag());
        }
        else
        {
            resetRequired = true;
        }
    }

    void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case AckFW.TYPE_ID:
            final AckFW ack = helper.ackRO.wrap(buffer, index, index + length);
            onAck(ack);
            break;
        default:
            // ignore
            break;
        }
    }

    void setCorrelatedThrottle(
        long correlatedStreamId,
        MessageConsumer correlatedThrottle)
    {
        this.correlatedThrottle = correlatedThrottle;
        this.correlatedStreamId = correlatedStreamId;
        if (resetRequired)
        {
            helper.doAck(correlatedThrottle, correlatedStreamId, RST.flag());
        }
    }

    private void onAck(
        AckFW ack)
    {
        final int flags = ack.flags();

        if (RST.check(flags))
        {
            onAck(flags);
        }
        else
        {
            if (ackIndex != -1)
            {
                ack.regions().forEach(this::onAckRegion);

                if (helper.readByteBuffer((int) (readIndex - ackIndex)).remaining() != 0)
                {
                    onNotifyReadable(key);
                }

                if (ackIndex != -1 &&
                    helper.readByteBuffer((int) (readIndex - ackIndex)).remaining() != 0)
                {
                    key.register(OP_READ);
                }
            }

            onAck(flags);
        }
    }

    private void onAck(
        int flags)
    {
        if (FIN.check(flags) || RST.check(flags))
        {
            try
            {
                if (RST.check(flags) && correlatedThrottle == null)
                {
                    try
                    {
                        // attempt to force a hard reset (TCP RST)
                        // "Orderly Versus Abortive Connection Release in Java"
                        // https://docs.oracle.com/javase/8/docs/technotes/guides/net/articles/connection_release.html
                        channel.setOption(StandardSocketOptions.SO_LINGER, 0);
                        channel.close();
                    }
                    catch (SocketException ex)
                    {
                        channel.shutdownInput();
                    }
                }
                else
                {
                    channel.shutdownInput();
                }
            }
            catch (IOException ex)
            {
                rethrowUnchecked(ex);
            }
            finally
            {
                helper.releaseReadMemory(memoryAddress);
            }
        }
    }

    private void onAckRegion(
        RegionFW region)
    {
        final long address = region.address();
        final int length = region.length();

        final long addressIndex = (ackIndex & ~readCapacityMask) | (address & readCapacityMask);

        ackIndexHighMark = Math.max(ackIndexHighMark, addressIndex + length);
        ackIndexProgress += length;

        final long ackIndexCandidate = ackIndex + ackIndexProgress;
        assert ackIndexCandidate <= ackIndexHighMark;
        if (ackIndexCandidate == ackIndexHighMark)
        {
            ackIndex = ackIndexCandidate;
            ackIndexHighMark = ackIndexCandidate;
            ackIndexProgress = 0;
        }
    }
}
