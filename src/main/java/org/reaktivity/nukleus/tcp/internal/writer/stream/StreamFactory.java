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

import static org.reaktivity.nukleus.tcp.internal.writer.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.tcp.internal.writer.stream.Slab.OUT_OF_MEMORY;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.writer.Source;
import org.reaktivity.nukleus.tcp.internal.writer.Target;

public final class StreamFactory
{
    // Mina uses a value of 256 (see AbstractPollingIoProcessor.writeBuffer).
    // Netty uses a configurable value, defaulting to 16
    // (see https://netty.io/4.0/api/io/netty/channel/ChannelConfig.html#setWriteSpinCount(int))
    public static final int WRITE_SPIN_COUNT = 16;

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final Source source;
    private final int windowSize;
    private final Slab writeSlab;
    private final LongSupplier incrementOverflow;

    public StreamFactory(
        Source source,
        int windowSize,
        int maxPartiallyWrittenStreams,
        LongSupplier incrementOverflow)
    {
        this.source = source;
        this.windowSize = windowSize;
        writeSlab = new Slab(maxPartiallyWrittenStreams, windowSize);
        this.incrementOverflow = incrementOverflow;
    }

    public MessageHandler newStream(
        long streamId,
        Target target,
        SocketChannel channel)
    {
        return new Stream(streamId, target, channel)::handleStream;
    }

    private final class Stream
    {
        private static final int EOS_REQUESTED = -1;
        private final long id;
        private final Target target;
        private final SocketChannel channel;
        private int slot = NO_SLOT;

        private SelectionKey key;
        private int readableBytes;

        private Stream(
            long id,
            Target target,
            SocketChannel channel)
        {
            this.id = id;
            this.target = target;
            this.channel = channel;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            try
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    processBegin(buffer, index, index + length);
                    break;
                case DataFW.TYPE_ID:
                    processData(buffer, index, index + length);
                    break;
                case EndFW.TYPE_ID:
                    processEnd(buffer, index, index + length);
                    break;
                default:
                    // ignore
                    break;
                }
            }
            catch (IOException ex)
            {
                doFail();
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            beginRO.wrap(buffer, offset, limit);

            this.key = target.doRegister(channel, 0, this::handleWrite);

            offerWindow(windowSize);
        }

        private void processData(
            DirectBuffer buffer,
            int offset,
            int limit) throws IOException
        {
            dataRO.wrap(buffer, offset, limit);

            final OctetsFW payload = dataRO.payload();
            final int writableBytes = dataRO.length();

            if (reduceWindow(writableBytes))
            {
                ByteBuffer writeBuffer = writeSlab.get(slot, buffer, payload.offset(), writableBytes);

                int bytesWritten = 0;

                for (int i = WRITE_SPIN_COUNT; bytesWritten == 0 && i > 0; i--)
                {
                    bytesWritten = channel.write(writeBuffer);
                }

                int originalSlot = slot;
                slot = writeSlab.written(id, slot, writeBuffer, bytesWritten, this::offerWindow);
                if (slot == OUT_OF_MEMORY)
                {
                    incrementOverflow.getAsLong();
                    doFail();
                    return;
                }
                if (bytesWritten < writableBytes)
                {
                    key.interestOps(SelectionKey.OP_WRITE);
                }
                else if (originalSlot != NO_SLOT)
                {
                    // we just flushed out a pending write
                    key.interestOps(0);
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
                    source.doReset(id);
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
                endRO.wrap(buffer, offset, limit);
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
            source.doReset(id);
            if (slot >= 0)
            {
                writeSlab.release(slot);
            }
            doCleanup();
        }

        private void doCleanup()
        {
            key.interestOps(0); // clear OP_WRITE
            try
            {
                source.removeStream(id);
                channel.shutdownOutput();
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        private int handleWrite()
        {
            key.interestOps(0); // clear OP_WRITE
            ByteBuffer writeBuffer = writeSlab.get(slot);

            int bytesWritten = 0;
            try
            {
                bytesWritten = channel.write(writeBuffer);
            }
            catch (IOException ex)
            {
                doFail();
                return 0;
            }

            slot = writeSlab.written(id, slot, writeBuffer, bytesWritten, this::offerWindow);

            if (slot == NO_SLOT)
            {
                if (readableBytes < 0) // deferred EOS and/or window was exceeded
                {
                    doCleanup();
                }
            }
            else
            {
                // Incomplete write
                key.interestOps(SelectionKey.OP_WRITE);
            }
            return bytesWritten;
        }

        private boolean reduceWindow(
            int update)
        {
            readableBytes -= update;
            return readableBytes >= 0;
        }

        private void offerWindow(final int update)
        {
            // If readableBytes indicates EOS has been received we must not destroy that information
            // (and in this case there is no need to write the window update)
            // We can also get update < 0 if we received data GT window (protocol violation) while
            // we have data waiting to be written (incomplete writes)
            if (readableBytes > EOS_REQUESTED)
            {
                readableBytes += update;
                source.doWindow(id, update);
            }
        }
    }
}
