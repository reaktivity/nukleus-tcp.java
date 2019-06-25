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

import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.TcpCounters;
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;

public class ServerStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final BeginFW beginRO = new BeginFW();

    private final RouteManager router;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongFunction<IntUnaryOperator> groupBudgetClaimer;
    private final LongFunction<IntUnaryOperator> groupBudgetReleaser;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Poller poller;

    private final BufferPool bufferPool;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final ByteBuffer writeByteBuffer;
    private final MessageWriter writer;
    private final int windowThreshold;

    final TcpCounters counters;

    public ServerStreamFactory(
        TcpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId,
        LongUnaryOperator supplyReplyId,
        Long2ObjectHashMap<Correlation> correlations,
        Poller poller,
        LongFunction<IntUnaryOperator> groupBudgetClaimer,
        LongFunction<IntUnaryOperator> groupBudgetReleaser,
        TcpCounters counters)
    {
        this.router = requireNonNull(router);
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.writer = new MessageWriter(supplyTypeId, requireNonNull(writeBuffer), requireNonNull(supplyTrace));
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.groupBudgetClaimer = requireNonNull(groupBudgetClaimer);
        this.groupBudgetReleaser = requireNonNull(groupBudgetReleaser);
        this.supplyReplyId = supplyReplyId;
        this.correlations = requireNonNull(correlations);
        this.counters = counters;

        int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;
        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
        this.poller = poller;
        this.windowThreshold = (bufferPool.slotCapacity() * config.windowThreshold()) / 100;
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
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
    }

    public void onAccepted(
        SocketChannel channel,
        InetSocketAddress address,
        LongFunction<InetSocketAddress> lookupAddress,
        Runnable onConnectionClosed)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final long routeId = route.correlationId();
            final InetSocketAddress routedAddress = lookupAddress.apply(routeId);
            return compareAddresses(address, routedAddress) == 0;
        };

        final RouteFW route = router.resolveExternal(0L, filter, this::wrapRoute);

        if (route != null)
        {
            final long routeId = route.correlationId();
            final long initialId = supplyInitialId.applyAsLong(routeId);
            final long replyId = supplyReplyId.applyAsLong(initialId);

            try
            {
                final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
                final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                final MessageConsumer receiver = router.supplyReceiver(initialId);

                final PollerKey key = poller.doRegister(channel, 0, null);

                final TcpRouteCounters routeCounters = counters.supplyRoute(routeId);

                final Runnable doCleanupConnection = () ->
                {
                    correlations.remove(replyId);
                    onConnectionClosed.run();
                };

                routeCounters.opensRead.getAsLong();
                routeCounters.opensWritten.getAsLong();

                final ReadStream stream = new ReadStream(receiver, routeId, initialId, key, channel,
                        readByteBuffer, readBuffer, writer, routeCounters, doCleanupConnection);
                final Correlation correlation = new Correlation(channel, stream,
                        receiver, initialId, routeCounters, onConnectionClosed);
                correlations.put(replyId, correlation);

                router.setThrottle(initialId, stream::handleThrottle);
                writer.doTcpBegin(receiver, routeId, initialId, localAddress, remoteAddress);

                final ToIntFunction<PollerKey> handler = stream::handleStream;

                key.handler(OP_READ, handler);
            }
            catch (IOException ex)
            {
                onConnectionClosed.run();
                CloseHelper.quietClose(channel);
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else
        {
            onConnectionClosed.run();
            CloseHelper.close(channel);
        }

    }

    private MessageConsumer newConnectReplyStream(
        BeginFW begin,
        MessageConsumer throttle)
    {
        MessageConsumer result = null;

        final long routeId = begin.routeId();
        final long replyId = begin.streamId();
        Correlation correlation = correlations.remove(replyId);

        if (correlation != null)
        {
            correlation.setCorrelatedThrottle(throttle, replyId);
            final SocketChannel channel = correlation.channel();

            final TcpRouteCounters counters = correlation.counters();
            final Runnable onConnectionClosed = correlation.onConnectionClosed();
            final WriteStream stream = new WriteStream(throttle, routeId, replyId, channel, poller,
                    bufferPool, writeByteBuffer, writer, counters, windowThreshold, onConnectionClosed);
            stream.setCorrelatedInput(correlation.correlatedStreamId(), correlation.correlatedStream());
            stream.onConnected();
            result = stream::handleStream;
        }
        else
        {
            writer.doReset(throttle, routeId, replyId);
        }

        return result;
    }

    private RouteFW wrapRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

}
