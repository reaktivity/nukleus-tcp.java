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

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

final class ReadStream
{
    private final MessageConsumer target;
    private final long streamId;
    private final PollerKey key;
    private final SocketChannel channel;
    private final ByteBuffer readBuffer;
    private final MutableDirectBuffer atomicBuffer;
    private final MessageWriter writer;
    private final LongSupplier frameCounter;
    private final LongConsumer bytesAccumulator;
    private Runnable connectionDone;
    private boolean closed;

    private MessageConsumer correlatedThrottle;
    private long correlatedStreamId;
    private int readableBytes;
    private int readPadding;
    private long readGroupId;
    private boolean resetRequired;

    ReadStream(
        MessageConsumer target,
        long streamId,
        PollerKey key,
        SocketChannel channel,
        ByteBuffer readByteBuffer,
        MutableDirectBuffer readBuffer,
        MessageWriter writer,
        LongSupplier frameCounter,
        LongConsumer bytesAccumulator,
        Runnable connectionDone)
    {
        this.target = target;
        this.streamId = streamId;
        this.key = key;
        this.channel = channel;
        this.readBuffer = readByteBuffer;
        this.atomicBuffer = readBuffer;
        this.writer = writer;
        this.frameCounter = frameCounter;
        this.bytesAccumulator = bytesAccumulator;
        this.connectionDone = connectionDone;
    }

    int handleStream(
        PollerKey key)
    {
        assert readableBytes > readPadding;

        final int limit = Math.min(readableBytes - readPadding, readBuffer.capacity());

        ((Buffer) readBuffer).position(0);
        ((Buffer) readBuffer).limit(limit);

        int bytesRead = 0;
        try
        {
            bytesRead = channel.read(readBuffer);
        }
        catch(IOException ex)
        {
            // TCP reset
            handleIOExceptionFromRead();
        }
        if (bytesRead == -1)
        {
            // channel input closed
            readableBytes = -1;
            writer.doTcpEnd(target, streamId);
            key.cancel(OP_READ);
            if (!closed)
            {
                closed = true;
                connectionDone.run();
            }
        }
        else if (bytesRead != 0)
        {
            frameCounter.getAsLong();
            bytesAccumulator.accept(bytesRead);
            // atomic buffer is zero copy with read buffer
            writer.doTcpData(target, streamId, readGroupId, readPadding, atomicBuffer, 0, bytesRead);

            readableBytes -= bytesRead + readPadding;

            if (readableBytes <= readPadding)
            {
                key.clear(OP_READ);
            }
        }

        return 1;
    }

    private void handleIOExceptionFromRead()
    {
        // IOException from read implies channel input and output will no longer function
        readableBytes = -1;
        writer.doTcpAbort(target, streamId);
        key.cancel(OP_READ);
        if (!closed)
        {
            closed = true;
            connectionDone.run();
        }
        if (correlatedThrottle != null)
        {
            writer.doReset(correlatedThrottle, correlatedStreamId);
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
        case WindowFW.TYPE_ID:
            final WindowFW window = writer.windowRO.wrap(buffer, index, index + length);
            processWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = writer.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    void setCorrelatedThrottle(long correlatedStreamId, MessageConsumer correlatedThrottle)
    {
        this.correlatedThrottle = correlatedThrottle;
        this.correlatedStreamId = correlatedStreamId;
        if (resetRequired)
        {
            writer.doReset(correlatedThrottle, correlatedStreamId);
        }
    }

    private void processWindow(
        WindowFW window)
    {
        if (readableBytes != -1)
        {
            readPadding = window.padding();
            readableBytes += window.credit();
            readGroupId = window.groupId();

            if (readableBytes > readPadding)
            {
                handleStream(key);
            }

            if (readableBytes > readPadding)
            {
                key.register(OP_READ);
            }
        }
    }

    private void processReset(
        ResetFW reset)
    {
        try
        {
            if (!closed)
            {
                closed = true;
                connectionDone.run();
            }

            if (correlatedThrottle != null)
            {
                // Begin on correlated WriteStream was already processed
                channel.shutdownInput();
            }
            else
            {
                // Force a hard reset (TCP RST), as documented in "Orderly Versus Abortive Connection Release in Java"
                // (https://docs.oracle.com/javase/8/docs/technotes/guides/net/articles/connection_release.html)
                channel.setOption(StandardSocketOptions.SO_LINGER, 0);
                channel.close();
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}