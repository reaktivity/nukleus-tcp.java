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

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.inetAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.TcpRouteExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.util.function.LongObjectBiConsumer;

public class ClientStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final TcpRouteExFW routeExRO = new TcpRouteExFW();
    private final BeginFW beginRO = new BeginFW();

    private final BufferPool bufferPool;
    private final LongSupplier incrementOverflow;
    private Poller poller;
    private final RouteManager router;
    private final LongSupplier supplyStreamId;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final ByteBuffer writeByteBuffer;
    private final MessageWriter writer;

    public ClientStreamFactory(
            Configuration configuration,
            RouteManager router,
            Poller poller,
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool,
            LongSupplier incrementOverflow,
            LongSupplier supplyStreamId)
    {
        this.router = requireNonNull(router);
        this.poller = poller;
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.bufferPool = requireNonNull(bufferPool);
        this.incrementOverflow = incrementOverflow;
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.writer = new MessageWriter(requireNonNull(writeBuffer));
        int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;

        // Data frame length must fit into a 2 byte unsigned integer
        readBufferSize = Math.min(readBufferSize, (1 << Short.SIZE) - 1);

        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer result = null;

        if (sourceRef == 0L)
        {
            final long sourceId = begin.streamId();
            writer.doReset(throttle, sourceId);
            throw new IllegalArgumentException(String.format("Stream id %d is not a connect stream, sourceRef is zero",
                    sourceId, sourceRef));
        }
        else
        {
            result = newAcceptStream(begin, throttle);
        }

        return result;
    }

    private MessageConsumer newAcceptStream(BeginFW begin, MessageConsumer throttle)
    {
        MessageConsumer result = null;
        final long streamId = begin.streamId();
        final String sourceName = begin.source().asString();
        final long sourceRef = begin.sourceRef();
        final long correlationId = begin.correlationId();

        MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef();
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        if (route != null)
        {
            final SocketChannel channel = newSocketChannel();
            final OctetsFW extension = routeRO.extension();
            String targetName = route.target().asString();
            long targetRef = route.targetRef();
            InetSocketAddress remoteAddress = null;
            if (extension.sizeof() > 0)
            {
                final TcpRouteExFW routeEx = extension.get(routeExRO::wrap);
                final InetAddress inetAddress = inetAddress(routeEx.address());
                remoteAddress = new InetSocketAddress(inetAddress, (int)sourceRef);
            }
            else
            {
                remoteAddress = new InetSocketAddress(targetName, (int)targetRef);
            }
            final WriteStream stream = new WriteStream(throttle, streamId, channel, poller, incrementOverflow,
                    bufferPool, writeByteBuffer, writer);
            result = stream::handleStream;

            doConnect(stream, channel, remoteAddress, sourceName, correlationId, throttle, streamId, stream::setCorrelatedInput);
        }
        else
        {
            writer.doReset(throttle, streamId);
    }

        return result;
    }

    private SocketChannel newSocketChannel()
    {
        try
        {
            final SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            return channel;
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }

    private RouteFW wrapRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }


    public void doConnect(
        WriteStream stream,
        SocketChannel channel,
        InetSocketAddress remoteAddress,
        String acceptReplyName,
        long correlationId,
        MessageConsumer outputThrottle,
        long outputStreamId,
        LongObjectBiConsumer<MessageConsumer> setCorrelatedInput)
    {
        final Request request = new Request(channel, stream, acceptReplyName, correlationId,
                outputThrottle, outputStreamId, setCorrelatedInput);

        try
        {
            if (channel.connect(remoteAddress))
            {
                handleConnected(request);
            }
            else
            {
                poller.doRegister(channel, OP_CONNECT, request);
            }
        }
        catch (IOException ex)
        {
            handleConnectFailed(request);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void handleConnected(Request request)
    {
        request.stream.doConnected();
        newConnectReplyStream(request);
    }

    private void newConnectReplyStream(Request request)
    {
        final SocketChannel channel = request.channel;
        final String targetName = request.acceptReplyName;
        final long targetId = supplyStreamId.getAsLong();
        final long correlationId = request.correlationId;
        final MessageConsumer correlatedThrottle = request.outputThrottle;
        final long correlatedStreamId = request.outputStreamdId;

        try
        {
            channel.setOption(TCP_NODELAY, true);
            final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            final MessageConsumer target = router.supplyTarget(targetName);
            request.setCorrelatedInput.accept(targetId, target);
            writer.doTcpBegin(target, targetId, 0L, correlationId, localAddress, remoteAddress);

            final PollerKey key = poller.doRegister(channel, 0, null);

            final ReadStream stream = new ReadStream(target, targetId, key, channel,
                    readByteBuffer, readBuffer, writer);
            stream.setCorrelatedThrottle(correlatedStreamId, correlatedThrottle);

            router.setThrottle(targetName, targetId, stream::handleThrottle);

            final ToIntFunction<PollerKey> handler = stream::handleStream;

            key.handler(OP_READ, handler);
        }
        catch (IOException ex)
        {
            CloseHelper.quietClose(channel);
            LangUtil.rethrowUnchecked(ex);
        }

    }

    private void handleConnectFailed(
        Request request)
    {
        request.stream.doConnectFailed();
    }

    private final class Request implements ToIntFunction<PollerKey>
    {
        private final WriteStream stream;
        private final SocketChannel channel;
        private final String acceptReplyName;
        private final long correlationId;
        private final MessageConsumer outputThrottle;
        private final long outputStreamdId;
        private final LongObjectBiConsumer<MessageConsumer> setCorrelatedInput;

        private Request(SocketChannel channel,
                        WriteStream stream,
                        String acceptReplyName,
                        long correlationId,
                        MessageConsumer outputThrottle,
                        long outputStreamId,
                        LongObjectBiConsumer<MessageConsumer> setCorrelatedInput)
        {
            this.channel = channel;
            this.stream = stream;
            this.acceptReplyName = acceptReplyName;
            this.correlationId = correlationId;
            this.outputThrottle = outputThrottle;
            this.outputStreamdId = outputStreamId;
            this.setCorrelatedInput = setCorrelatedInput;
        }

        @Override
        public String toString()
        {
            return String.format("[writeStream=%s]", stream);
        }

        @Override
        public int applyAsInt(
            PollerKey key)
        {
            try
            {
                channel.finishConnect();
                handleConnected(this);
            }
            catch (IOException ex)
            {
                handleConnectFailed(this);
            }
            finally
            {
                key.cancel(OP_CONNECT);
            }

            return 1;
        }
    }


}
