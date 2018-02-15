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

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;

public class ServerStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final BeginFW beginRO = new org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW();

    private final RouteManager router;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Poller poller;

    private final LongSupplier countOverflows;
    private final ByteBuffer writeByteBuffer;
    private final StreamHelper helper;

    private final Function<RouteFW, LongSupplier> supplyWriteFrameCounter;
    private final Function<RouteFW, LongSupplier> supplyReadFrameCounter;
    private final Function<RouteFW, LongConsumer> supplyWriteBytesCounter;
    private final Function<RouteFW, LongConsumer> supplyReadBytesCounter;

    public ServerStreamFactory(
        TcpConfiguration configuration,
        RouteManager router,
        Poller poller,
        MemoryManager memory,
        MutableDirectBuffer writeBuffer,
        Supplier<DirectBufferBuilder> supplyDirectBufferBuilder,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        LongSupplier countOverflows,
        Function<RouteFW, LongSupplier> supplyReadFrameCounter,
        Function<RouteFW, LongConsumer> supplyReadBytesCounter,
        Function<RouteFW, LongSupplier> supplyWriteFrameCounter,
        Function<RouteFW, LongConsumer> supplyWriteBytesCounter)
    {
        this.router = requireNonNull(router);
        this.poller = poller;
        this.countOverflows = countOverflows;
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);

        this.supplyWriteFrameCounter = supplyWriteFrameCounter;
        this.supplyReadFrameCounter = supplyReadFrameCounter;
        this.supplyWriteBytesCounter = supplyWriteBytesCounter;
        this.supplyReadBytesCounter = supplyReadBytesCounter;

        final int transferCapacity = configuration.transferCapacity();
        final int pendingRegionsCapacity = configuration.pendingRegionsCapacity();

        final DirectBufferBuilder directBufferBuilder = supplyDirectBufferBuilder.get();
        this.helper = new StreamHelper(memory, directBufferBuilder, writeBuffer, transferCapacity, pendingRegionsCapacity);
        this.writeByteBuffer = allocateDirect(writeBuffer.capacity()).order(nativeOrder());
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

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            final long sourceId = begin.streamId();
            helper.doAck(throttle, sourceId, 0x02);
            throw new IllegalArgumentException(String.format("Stream id %d is not a reply stream, sourceRef %d is non-zero",
                    sourceId, sourceRef));
        }

        return newStream;
    }

    public void onAccepted(String sourceName, long sourceRef, SocketChannel channel, InetSocketAddress address)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            InetAddress inetAddress = null;
            InetSocketAddress routedAddress = new InetSocketAddress(inetAddress, (int)sourceRef);
            return sourceRef == route.sourceRef() &&
                    sourceName.equals(route.source().asString()) &&
                         compareAddresses(address, routedAddress) == 0;
        };

        final RouteFW route = router.resolve(0L, filter, this::wrapRoute);

        if (route != null)
        {
            final long targetRef = route.targetRef();
            final String targetName = route.target().asString();
            final long targetId = supplyStreamId.getAsLong();
            final long correlationId = supplyCorrelationId.getAsLong();

            try
            {
                final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
                final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                final MessageConsumer target = router.supplyTarget(targetName);
                helper.doTcpBegin(target, targetId, targetRef, correlationId, localAddress, remoteAddress);

                final PollerKey key = poller.doRegister(channel, 0, null);

                final LongSupplier countWriteFrames = supplyWriteFrameCounter.apply(route);
                final LongConsumer countWriteBytes = supplyWriteBytesCounter.apply(route);
                final LongSupplier countReadFrames = supplyReadFrameCounter.apply(route);
                final LongConsumer countReadBytes = supplyReadBytesCounter.apply(route);

                final ReadStream stream = new ReadStream(target, targetId, key, channel,
                        helper, countWriteFrames, countWriteBytes);
                final Correlation correlation = new Correlation(sourceName, channel, stream::setCorrelatedThrottle,
                        target, targetId, countReadFrames, countReadBytes);
                correlations.put(correlationId, correlation);

                router.setThrottle(targetName, targetId, stream::handleThrottle);
                key.handler(OP_READ, stream::onNotifyReadable);
            }
            catch (IOException ex)
            {
                CloseHelper.quietClose(channel);
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else
        {
            CloseHelper.close(channel);
        }

    }

    private MessageConsumer newConnectReplyStream(BeginFW begin, MessageConsumer throttle)
    {
        MessageConsumer result = null;
        final long correlationId = begin.correlationId();
        Correlation correlation = correlations.remove(correlationId);
        final long streamId = begin.streamId();

        if (correlation != null)
        {
            correlation.setCorrelatedThrottle(throttle, streamId);
            final SocketChannel channel = correlation.channel();

            final LongSupplier countFrames = correlation.countFrames();
            final LongConsumer countBytes = correlation.countBytes();
            final WriteStream stream = new WriteStream(throttle, streamId, channel, poller,
                    writeByteBuffer, helper, countOverflows, countFrames, countBytes);
            stream.setCorrelatedInput(correlation.correlatedStreamId(), correlation.correlatedStream());
            stream.onConnected();
            result = stream::handleStream;
        }
        else
        {
            helper.doAck(throttle, streamId, 0x02);
        }

        return result;
    }

    private RouteFW wrapRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

}
