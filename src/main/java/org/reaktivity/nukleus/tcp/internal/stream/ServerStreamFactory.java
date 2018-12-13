/**
 * Copyright 2016-2018 The Reaktivity Project
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
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
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;
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
        LongSupplier supplyStreamId,
        LongSupplier supplyTrace,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        Poller poller,
        LongFunction<IntUnaryOperator> groupBudgetClaimer,
        LongFunction<IntUnaryOperator> groupBudgetReleaser,
        TcpCounters counters)
    {
        this.router = requireNonNull(router);
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.writer = new MessageWriter(requireNonNull(writeBuffer), requireNonNull(supplyTrace));
        this.bufferPool = bufferPool;
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.groupBudgetClaimer = requireNonNull(groupBudgetClaimer);
        this.groupBudgetReleaser = requireNonNull(groupBudgetReleaser);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
        this.counters = counters;

        int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;

        // Data frame length must fit into a 2 byte unsigned integer
        readBufferSize = Math.min(readBufferSize, (1 << Short.SIZE) - 1);

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
        final long routeId = begin.routeId();
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            final long sourceId = begin.streamId();
            writer.doReset(throttle, routeId, sourceId);
            throw new IllegalArgumentException(String.format("Stream id %d is not a reply stream, sourceRef %d is non-zero",
                    sourceId, sourceRef));
        }

        return newStream;
    }

    public void onAccepted(
        String sourceName,
        long sourceRef,
        SocketChannel channel,
        InetSocketAddress address,
        Runnable onConnectionClosed)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            InetAddress inetAddress = null;
            InetSocketAddress routedAddress = new InetSocketAddress(inetAddress, (int)sourceRef);
            return sourceRef == route.sourceRef() &&
                    sourceName.equals(route.source().asString()) &&
                         compareAddresses(address, routedAddress) == 0;
        };

        final RouteFW route = router.resolve(0L, filter, this::wrapRoute);

        if (route != null)
        {
            final long routeId = route.correlationId();
            final long targetRef = route.targetRef();
            final String targetName = route.target().asString();
            final long targetId = supplyStreamId.getAsLong();
            final long correlationId = supplyCorrelationId.getAsLong();

            try
            {
                final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
                final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                final MessageConsumer target = router.supplyTarget(targetName);
                writer.doTcpBegin(target, routeId, targetId, targetRef, correlationId, localAddress, remoteAddress);

                final PollerKey key = poller.doRegister(channel, 0, null);

                final TcpRouteCounters routeCounters = counters.supplyRoute(route.correlationId());

                final Runnable doCleanupConnection = () ->
                {
                    correlations.remove(correlationId);
                    onConnectionClosed.run();
                };

                routeCounters.opensRead.getAsLong();
                routeCounters.opensWritten.getAsLong();

                final ReadStream stream = new ReadStream(target, routeId, targetId, key, channel,
                        readByteBuffer, readBuffer, writer, routeCounters, doCleanupConnection);
                final Correlation correlation = new Correlation(sourceName, channel, stream,
                        target, targetId, routeCounters, onConnectionClosed);
                correlations.put(correlationId, correlation);

                router.setThrottle(targetName, targetId, stream::handleThrottle);

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
        final long streamId = begin.streamId();
        final long correlationId = begin.correlationId();
        Correlation correlation = correlations.remove(correlationId);

        if (correlation != null)
        {
            correlation.setCorrelatedThrottle(throttle, streamId);
            final SocketChannel channel = correlation.channel();

            final TcpRouteCounters counters = correlation.counters();
            final Runnable onConnectionClosed = correlation.onConnectionClosed();
            final WriteStream stream = new WriteStream(throttle, routeId, streamId, channel, poller,
                    bufferPool, writeByteBuffer, writer, counters, windowThreshold, onConnectionClosed);
            stream.setCorrelatedInput(correlation.correlatedStreamId(), correlation.correlatedStream());
            stream.onConnected();
            result = stream::handleStream;
        }
        else
        {
            writer.doReset(throttle, routeId, streamId);
        }

        return result;
    }

    private RouteFW wrapRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

}
