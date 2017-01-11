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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

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
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final Source source;
    private final int windowSize;
    private final ByteBuffer writeBuffer;

    public StreamFactory(
        Source source,
        int windowSize,
        int maxMessageSize)
    {
        this.source = source;
        this.windowSize = windowSize;
        this.writeBuffer = ByteBuffer.allocateDirect(maxMessageSize);
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
        private final long id;
        private final Target target;
        private final SocketChannel channel;

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
                LangUtil.rethrowUnchecked(ex);
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
            final int writableBytes = Math.max(payload.length() - 1, 0);

            if (reduceWindow(writableBytes))
            {
                writeBuffer.position(0);
                writeBuffer.limit(writeBuffer.capacity());
                buffer.getBytes(payload.offset() + 1, writeBuffer, writableBytes);
                writeBuffer.flip();

                final int bytesWritten = channel.write(writeBuffer);

                if (bytesWritten < writableBytes)
                {
                    key.interestOps(SelectionKey.OP_WRITE);
                    throw new IOException("partial write, defer unwritten bytes");
                }

                offerWindow(bytesWritten);
            }
            else
            {
                doFail();
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            endRO.wrap(buffer, offset, limit);

            // TODO: flush partial writes first (if necessary)
            doCleanup();
        }

        private void doFail()
        {
            source.doReset(id);
            doCleanup();
        }

        private void doCleanup()
        {
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
            // TODO: partial write completed

            // TODO: end stream (if necessary) after all partial writes completed
            return 0;
        }

        private boolean reduceWindow(
            int update)
        {
            readableBytes -= update;
            return readableBytes >= 0;
        }

        private void offerWindow(
            final int update)
        {
            readableBytes += update;
            source.doWindow(id, update);
        }
    }
}
