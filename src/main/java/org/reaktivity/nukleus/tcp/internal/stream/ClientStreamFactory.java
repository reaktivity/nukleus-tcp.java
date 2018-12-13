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

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.Objects.requireNonNull;
import static org.agrona.LangUtil.rethrowUnchecked;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
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
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.TcpAddressFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.tcp.internal.util.CIDR;
import org.reaktivity.nukleus.tcp.internal.util.function.LongObjectBiConsumer;

public class ClientStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final TcpBeginExFW tcpBeginExRO = new TcpBeginExFW();
    private final BeginFW beginRO = new BeginFW();

    private final BufferPool bufferPool;
    private Poller poller;
    private final RouteManager router;
    private final LongUnaryOperator supplyReplyId;
    private final LongFunction<IntUnaryOperator> groupBudgetClaimer;
    private final LongFunction<IntUnaryOperator> groupBudgetReleaser;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final ByteBuffer writeByteBuffer;
    private final MessageWriter writer;
    private final Map<String, Predicate<? super InetAddress>> targetToCidrMatch;
    private final TcpCounters counters;
    private final int windowThreshold;

    public ClientStreamFactory(
        TcpConfiguration configuration,
        RouteManager router,
        Poller poller,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTrace,
        LongFunction<IntUnaryOperator> groupBudgetClaimer,
        LongFunction<IntUnaryOperator> groupBudgetReleaser,
        TcpCounters counters)
    {
        this.router = requireNonNull(router);
        this.poller = poller;
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.groupBudgetClaimer = requireNonNull(groupBudgetClaimer);
        this.groupBudgetReleaser = requireNonNull(groupBudgetReleaser);
        this.writer = new MessageWriter(requireNonNull(writeBuffer), requireNonNull(supplyTrace));
        int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;

        // Data frame length must fit into a 2 byte unsigned integer
        readBufferSize = Math.min(readBufferSize, (1 << Short.SIZE) - 1);

        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
        this.targetToCidrMatch = new HashMap<>();

        this.counters = counters;
        this.windowThreshold = (bufferPool.slotCapacity() * configuration.windowThreshold()) / 100;
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

        if (sourceRef != 0L)
        {
            result = newAcceptStream(begin, throttle);
        }

        return result;
    }

    private MessageConsumer newAcceptStream(
        BeginFW begin,
        MessageConsumer throttle)
    {
        MessageConsumer result = null;
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final String sourceName = begin.source().asString();
        final long sourceRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        final OctetsFW extension = begin.extension();
        final boolean hasExtension = extension.sizeof() > 0;

        MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final String targetName = route.target().asString();
            final long targetRef = route.targetRef();
            return sourceRef == route.sourceRef() &&
                   (!hasExtension || resolveRemoteAddressExt(extension, targetName, targetRef) != null);
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        if (route != null)
        {
            final SocketChannel channel = newSocketChannel();
            final long targetRouteId = route.correlationId();
            String targetName = route.target().asString();
            long targetRef = route.targetRef();

            InetSocketAddress remoteAddress = hasExtension ? resolveRemoteAddressExt(extension, targetName, targetRef) :
                                                             new InetSocketAddress(targetName, (int)targetRef);
            assert remoteAddress != null;

            final TcpRouteCounters routeCounters = counters.supplyRoute(targetRouteId);

            final WriteStream stream = new WriteStream(throttle, routeId, streamId, channel, poller,
                    bufferPool, writeByteBuffer, writer, routeCounters, windowThreshold, () -> {});
            result = stream::handleStream;

            doConnect(
                stream,
                channel,
                remoteAddress,
                sourceName,
                correlationId,
                routeId,
                throttle,
                streamId,
                stream::setCorrelatedInput,
                targetRouteId,
                routeCounters);
        }

        return result;

 }

    private InetSocketAddress resolveRemoteAddressExt(
            OctetsFW extension,
            String targetName,
            long targetRef)
    {
        InetSocketAddress result = null;
        InetAddress address = null;

        try
        {
            final TcpBeginExFW beginEx = extension.get(tcpBeginExRO::wrap);
            TcpAddressFW remoteAddressExt = beginEx.remoteAddress();
            Predicate<? super InetAddress> subnetFilter = extensionMatcher(targetName);

            int remotePort = beginEx.remotePort();

            if (targetRef == 0 || targetRef == remotePort)
            {

                switch(remoteAddressExt.kind())
                {
                    case TcpAddressFW.KIND_HOST:
                        String requestedAddressName = remoteAddressExt.host().asString();
                        Optional<InetAddress> optional = Arrays
                                .stream(InetAddress.getAllByName(requestedAddressName))
                                .filter(subnetFilter)
                                .findFirst();
                        address = optional.isPresent() ? optional.get() : null;
                        break;
                    case TcpAddressFW.KIND_IPV4_ADDRESS:
                        OctetsFW ipRO = remoteAddressExt.ipv4Address();
                        byte[] addr = new byte[ipRO.sizeof()];
                        ipRO.buffer().getBytes(ipRO.offset(), addr, 0, ipRO.sizeof());
                        InetAddress candidate = InetAddress.getByAddress(addr);
                        address =  subnetFilter.test(candidate) ? candidate: null;
                        break;
                    case TcpAddressFW.KIND_IPV6_ADDRESS:
                        ipRO = remoteAddressExt.ipv6Address();
                        addr = new byte[ipRO.sizeof()];
                        ipRO.buffer().getBytes(ipRO.offset(), addr, 0, ipRO.sizeof());
                        candidate = InetAddress.getByAddress(addr);
                        address =  subnetFilter.test(candidate) ? candidate: null;
                        break;
                    default:
                        throw new RuntimeException("Unexpected address kind");
                }
                if (address != null)
                {
                    result = new InetSocketAddress(address, remotePort);
                }
            }
        }
        catch (UnknownHostException ignore)
        {
           // NOOP
        }
        return result;
    }

    private Predicate<? super InetAddress> extensionMatcher(String targetName) throws UnknownHostException
    {
        Predicate<? super InetAddress> result;
        if (targetName.contains("/"))
        {
            result = targetToCidrMatch.computeIfAbsent(targetName, this::inetMatchesCIDR);
        }
        else
        {
            InetAddress.getByName(targetName);
            result = targetToCidrMatch.computeIfAbsent(targetName, this::inetMatchesInet);
        }
        return result;
    }

    private Predicate<InetAddress> inetMatchesCIDR(String targetName)
    {
        final CIDR cidr = new CIDR(targetName);
        return candidate -> cidr.isInRange(candidate.getHostAddress());
    }

    private Predicate<InetAddress> inetMatchesInet(String targetName)
    {
        try
        {
            InetAddress toMatch = InetAddress.getByName(targetName);
            return candidate -> toMatch.equals(candidate);
        }
        catch (UnknownHostException e)
        {
            rethrowUnchecked(e);
        }
        return candidate -> false;
    }

    private SocketChannel newSocketChannel()
    {
        try
        {
            final SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.setOption(TCP_NODELAY, true);
            return channel;
        }
        catch (IOException ex)
        {
            rethrowUnchecked(ex);
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
        long outputRouteId,
        MessageConsumer outputThrottle,
        long outputStreamId,
        LongObjectBiConsumer<MessageConsumer> setCorrelatedInput,
        long targetRouteId,
        TcpRouteCounters counters)
    {
        final Request request = new Request(channel, stream, acceptReplyName, correlationId,
                outputRouteId, outputThrottle, outputStreamId, targetRouteId, setCorrelatedInput);

        try
        {
            counters.opensWritten.getAsLong();
            if (channel.connect(remoteAddress))
            {
                handleConnected(request);
            }
            else
            {
                poller.doRegister(channel, OP_CONNECT, request);
            }
        }
        catch (UnresolvedAddressException | IOException ex)
        {
            handleConnectFailed(request);
        }
    }

    private void handleConnected(
        Request request)
    {
        request.stream.onConnected();
        newConnectReplyStream(request);
    }

    private void newConnectReplyStream(
        Request request)
    {
        final SocketChannel channel = request.channel;
        final MessageConsumer acceptThrottle = request.acceptThrottle;
        final String acceptReplyName = request.acceptReplyName;
        final long acceptRouteId = request.acceptRouteId;
        final long acceptInitialId = request.acceptInitialId;
        final long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
        final long acceptCorrelationId = request.acceptCorrelationId;

        try
        {
            final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            final MessageConsumer acceptReply = router.supplyTarget(acceptReplyName);
            request.setCorrelatedInput.accept(acceptReplyId, acceptReply);
            writer.doTcpBegin(acceptReply, acceptRouteId, acceptReplyId, 0L, acceptCorrelationId, localAddress, remoteAddress);

            final PollerKey key = poller.doRegister(channel, 0, null);

            final TcpRouteCounters routeCounters = counters.supplyRoute(request.connectRouteId);

            final ReadStream stream = new ReadStream(acceptReply, acceptRouteId, acceptReplyId, key, channel,
                    readByteBuffer, readBuffer, writer, routeCounters, () -> {});
            stream.setCorrelatedThrottle(acceptInitialId, acceptThrottle);

            router.setThrottle(acceptReplyName, acceptReplyId, stream::handleThrottle);

            final ToIntFunction<PollerKey> handler = stream::handleStream;

            key.handler(OP_READ, handler);
        }
        catch (IOException ex)
        {
            CloseHelper.quietClose(channel);
            rethrowUnchecked(ex);
        }

    }

    private void handleConnectFailed(
        Request request)
    {
        request.stream.onConnectFailed();
    }

    private final class Request implements ToIntFunction<PollerKey>
    {
        private final WriteStream stream;
        private final SocketChannel channel;
        private final String acceptReplyName;
        private final long acceptCorrelationId;
        private final MessageConsumer acceptThrottle;
        private final long acceptRouteId;
        private final long acceptInitialId;
        private final long connectRouteId;
        private final LongObjectBiConsumer<MessageConsumer> setCorrelatedInput;

        private Request(
            SocketChannel channel,
            WriteStream stream,
            String acceptReplyName,
            long correlationId,
            long outputRouteId,
            MessageConsumer outputThrottle,
            long outputStreamId,
            long targetRouteId,
            LongObjectBiConsumer<MessageConsumer> setCorrelatedInput)
        {
            this.channel = channel;
            this.stream = stream;
            this.acceptReplyName = acceptReplyName;
            this.acceptCorrelationId = correlationId;
            this.acceptRouteId = outputRouteId;
            this.acceptThrottle = outputThrottle;
            this.acceptInitialId = outputStreamId;
            this.connectRouteId = targetRouteId;
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
            catch (UnresolvedAddressException | IOException ex)
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
