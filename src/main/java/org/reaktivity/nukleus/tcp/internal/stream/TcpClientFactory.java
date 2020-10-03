/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.lang.Integer.parseInt;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.Objects.requireNonNull;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.tcp.internal.TcpNukleus.WRITE_SPIN_COUNT;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.CONNECT_HOST_AND_PORT_PATTERN;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.socketAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.TcpCounters;
import org.reaktivity.nukleus.tcp.internal.TcpNukleus;
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.TcpAddressFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.tcp.internal.util.CIDR;

public class TcpClientFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private final TcpBeginExFW beginExRO = new TcpBeginExFW();
    private final TcpBeginExFW.Builder beginExRW = new TcpBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final BufferPool bufferPool;
    private Poller poller;
    private final RouteManager router;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer writeByteBuffer;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final int tcpTypeId;
    private final Map<String, Predicate<? super InetAddress>> targetToCidrMatch;
    private final TcpCounters counters;
    private final int windowThreshold;
    private final boolean keepalive;

    public TcpClientFactory(
        TcpConfiguration config,
        RouteManager router,
        Poller poller,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        TcpCounters counters)
    {
        this.router = requireNonNull(router);
        this.poller = poller;
        this.writeBuffer = requireNonNull(writeBuffer);
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.tcpTypeId = supplyTypeId.applyAsInt(TcpNukleus.NAME);

        final int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;
        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
        this.targetToCidrMatch = new HashMap<>();

        this.counters = counters;
        this.windowThreshold = (bufferPool.slotCapacity() * config.windowThreshold()) / 100;
        this.keepalive = config.keepalive();
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

        MessageConsumer result = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            result = newInitialStream(begin, throttle);
        }

        return result;
    }

    private MessageConsumer newInitialStream(
        BeginFW begin,
        MessageConsumer application)
    {
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final OctetsFW extension = begin.extension();
        final boolean hasExtension = extension.sizeof() > 0;

        MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final String remoteAddressAndPort = route.remoteAddress().asString();
            final Matcher matcher = CONNECT_HOST_AND_PORT_PATTERN.matcher(remoteAddressAndPort);
            return !hasExtension ||
                    (matcher.matches() &&
                            resolveRemoteAddressExt(extension, matcher.group(1),
                                                               parseInt(matcher.group(2))) != null);
        };

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final String remoteAddressAndPort = route.remoteAddress().asString();
            final Matcher matcher = CONNECT_HOST_AND_PORT_PATTERN.matcher(remoteAddressAndPort);
            matcher.matches();
            final String remoteHost = matcher.group(1);
            final int remotePort = parseInt(matcher.group(2));
            InetSocketAddress remoteAddress = hasExtension ? resolveRemoteAddressExt(extension, remoteHost, remotePort) :
                                                             new InetSocketAddress(remoteHost, remotePort);
            assert remoteAddress != null;

            final SocketChannel channel = newSocketChannel();
            final TcpRouteCounters routeCounters = counters.supplyRoute(route.correlationId());

            final TcpClient client = new TcpClient(application, routeId, initialId, channel, routeCounters);
            client.doNetworkConnect(remoteAddress);
            newStream = client::onApplication;
        }

        return newStream;
    }

    private InetSocketAddress resolveRemoteAddressExt(
        OctetsFW extension,
        String targetName,
        long targetRef)
    {
        final TcpBeginExFW beginEx = extension.get(beginExRO::wrap);
        final TcpAddressFW remoteAddress = beginEx.remoteAddress();
        final int remotePort = beginEx.remotePort();

        InetAddress address = null;
        try
        {
            Predicate<? super InetAddress> subnetFilter = extensionMatcher(targetName);

            if (targetRef == 0 || targetRef == remotePort)
            {
                switch (remoteAddress.kind())
                {
                case TcpAddressFW.KIND_HOST:
                    String requestedAddressName = remoteAddress.host().asString();
                    Optional<InetAddress> optional = Arrays
                            .stream(InetAddress.getAllByName(requestedAddressName))
                            .filter(subnetFilter)
                            .findFirst();
                    address = optional.isPresent() ? optional.get() : null;
                    break;
                case TcpAddressFW.KIND_IPV4_ADDRESS:
                    OctetsFW ipRO = remoteAddress.ipv4Address();
                    byte[] addr = new byte[ipRO.sizeof()];
                    ipRO.buffer().getBytes(ipRO.offset(), addr, 0, ipRO.sizeof());
                    InetAddress candidate = InetAddress.getByAddress(addr);
                    address =  subnetFilter.test(candidate) ? candidate : null;
                    break;
                case TcpAddressFW.KIND_IPV6_ADDRESS:
                    ipRO = remoteAddress.ipv6Address();
                    addr = new byte[ipRO.sizeof()];
                    ipRO.buffer().getBytes(ipRO.offset(), addr, 0, ipRO.sizeof());
                    candidate = InetAddress.getByAddress(addr);
                    address =  subnetFilter.test(candidate) ? candidate : null;
                    break;
                default:
                    throw new RuntimeException("Unexpected address kind");
                }
            }
        }
        catch (UnknownHostException ignore)
        {
           // NOOP
        }

        return address != null ? new InetSocketAddress(address, remotePort) : null;
    }

    private Predicate<? super InetAddress> extensionMatcher(
        String targetName) throws UnknownHostException
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

    private Predicate<InetAddress> inetMatchesCIDR(
        String targetName)
    {
        final CIDR cidr = new CIDR(targetName);
        return candidate -> cidr.isInRange(candidate.getHostAddress());
    }

    private Predicate<InetAddress> inetMatchesInet(
        String targetName)
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

    private void doCloseNetwork(
        SocketChannel network)
    {
        CloseHelper.quietClose(network);
    }

    private final class TcpClient
    {
        private final MessageConsumer application;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final SocketChannel network;
        private final TcpRouteCounters counters;

        private PollerKey networkKey;

        private long replyBudgetId;
        private int replyBudget;
        private int replyPadding;

        private int initialBudget;

        private int state;
        private int networkSlot = NO_SLOT;
        private int networkSlotOffset;
        private int bytesFlushed;

        private TcpClient(
            MessageConsumer application,
            long routeId,
            long initialId,
            SocketChannel network,
            TcpRouteCounters counters)
        {
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.network = network;
            this.counters = counters;
        }

        private void doNetworkConnect(
            InetSocketAddress remoteAddress)
        {
            try
            {
                state = TcpState.openingInitial(state);
                counters.opensWritten.getAsLong();
                network.setOption(SO_KEEPALIVE, keepalive);

                if (network.connect(remoteAddress))
                {
                    onNetworkConnected();
                }
                else
                {
                    networkKey = poller.doRegister(network, OP_CONNECT, this::onNetworkConnect);
                }
            }
            catch (UnresolvedAddressException | IOException ex)
            {
                onNetworkRejected();
            }
        }

        private int onNetworkConnect(
            PollerKey key)
        {
            try
            {
                key.clear(OP_CONNECT);
                network.finishConnect();
                onNetworkConnected();
            }
            catch (UnresolvedAddressException | IOException ex)
            {
                onNetworkRejected();
            }

            return 1;
        }

        private void onNetworkConnected()
        {
            final long traceId = supplyTraceId.getAsLong();

            state = TcpState.openInitial(state);
            counters.opensRead.getAsLong();

            try
            {
                networkKey.handler(OP_READ, this::onNetworkReadable);
                networkKey.handler(OP_WRITE, this::onNetworkWritable);

                doApplicationBegin(traceId);
                doApplicationWindow(traceId, bufferPool.slotCapacity());
            }
            catch (IOException ex)
            {
                doCleanup(traceId);
            }
        }

        private void onNetworkRejected()
        {
            final long traceId = supplyTraceId.getAsLong();

            counters.resetsRead.getAsLong();

            doCleanup(traceId);
        }

        private int onNetworkReadable(
            PollerKey key)
        {
            assert replyBudget > replyPadding;

            final int limit = Math.min(replyBudget - replyPadding, readBuffer.capacity());

            ((Buffer) readByteBuffer).position(0);
            ((Buffer) readByteBuffer).limit(limit);

            try
            {
                final int bytesRead = network.read(readByteBuffer);

                if (bytesRead == -1)
                {
                    key.clear(OP_READ);
                    CloseHelper.close(network::shutdownInput);
                    counters.closesRead.getAsLong();

                    doApplicationEnd(supplyTraceId.getAsLong());

                    if (network.socket().isOutputShutdown())
                    {
                        doCloseNetwork(network);
                    }
                }
                else if (bytesRead != 0)
                {
                    counters.bytesRead.accept(bytesRead);
                    doApplicationData(readBuffer, 0, bytesRead);
                }
            }
            catch (IOException ex)
            {
                doCleanup(supplyTraceId.getAsLong());
            }

            return 1;
        }

        private int onNetworkWritable(
            PollerKey key)
        {
            if (networkSlot == NO_SLOT)
            {
                counters.writeopsNoSlot.getAsLong();
                assert key == networkKey;
                return 0;
            }
            else
            {
                assert networkSlot != NO_SLOT;

                long traceId = supplyTraceId.getAsLong();
                DirectBuffer buffer = bufferPool.buffer(networkSlot);
                ByteBuffer byteBuffer = bufferPool.byteBuffer(networkSlot);
                byteBuffer.limit(byteBuffer.position() + networkSlotOffset);

                return doNetworkWrite(buffer, 0, networkSlotOffset, byteBuffer, traceId);
            }
        }

        private int doNetworkWrite(
            DirectBuffer buffer,
            int offset,
            int length,
            ByteBuffer byteBuffer,
            long traceId)
        {
            int bytesWritten = 0;

            try
            {
                for (int i = WRITE_SPIN_COUNT; bytesWritten == 0 && i > 0; i--)
                {
                    bytesWritten = network.write(byteBuffer);
                }

                counters.bytesWritten.accept(bytesWritten);

                bytesFlushed += bytesWritten;

                if (bytesWritten < length)
                {
                    if (networkSlot == NO_SLOT)
                    {
                        networkSlot = bufferPool.acquire(initialId);
                    }

                    if (networkSlot == NO_SLOT)
                    {
                        counters.overflows.getAsLong();
                        doApplicationResetIfNecessary(traceId);
                        doCleanup(traceId);
                    }
                    else
                    {
                        final MutableDirectBuffer slotBuffer = bufferPool.buffer(networkSlot);
                        slotBuffer.putBytes(0, buffer, offset + bytesWritten, length - bytesWritten);
                        networkSlotOffset = length - bytesWritten;

                        networkKey.register(OP_WRITE);
                        counters.writeops.getAsLong();
                    }
                }
                else
                {
                    cleanupNetworkSlotIfNecessary();
                    networkKey.clear(OP_WRITE);

                    if (TcpState.initialClosing(state))
                    {
                        doNetworkShutdownOutput(traceId);
                    }
                    else if (bytesFlushed >= windowThreshold)
                    {
                        doApplicationWindow(traceId, bytesFlushed);
                        bytesFlushed = 0;
                    }
                }
            }
            catch (IOException ex)
            {
                doCleanup(traceId);
            }

            return bytesWritten;
        }

        private void doNetworkShutdownOutput(
            long traceId)
        {
            state = TcpState.closeInitial(state);

            cleanupNetworkSlotIfNecessary();

            try
            {
                if (network.isConnectionPending())
                {
                    networkKey.clear(OP_CONNECT);
                    doCloseNetwork(network);
                    counters.closesWritten.getAsLong();
                }
                else
                {
                    networkKey.clear(OP_WRITE);
                    network.shutdownOutput();
                    counters.closesWritten.getAsLong();

                    if (network.socket().isInputShutdown())
                    {
                        doCloseNetwork(network);
                    }
                }
            }
            catch (IOException ex)
            {
                doCleanup(traceId);
            }
        }

        private void cleanupNetworkSlotIfNecessary()
        {
            if (networkSlot != NO_SLOT)
            {
                bufferPool.release(networkSlot);
                networkSlot = NO_SLOT;
                networkSlotOffset = 0;
            }
        }

        private void onApplication(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onApplicationBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onApplicationData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onApplicationEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onApplicationAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onApplicationReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onApplicationWindow(window);
                break;
            }
        }

        private void onApplicationBegin(
            BeginFW begin)
        {
            assert TcpState.initialOpening(state);
        }

        private void onApplicationData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();

            initialBudget -= reserved;

            if (initialBudget < 0)
            {
                doApplicationResetIfNecessary(traceId);
                doCleanup(traceId);
            }
            else
            {
                final OctetsFW payload = data.payload();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int length = payload.sizeof();

                assert reserved == length;
                assert length > 0;

                ByteBuffer byteBuffer;

                if (networkSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(networkSlot);
                    slotBuffer.putBytes(networkSlotOffset, buffer, offset, length);
                    networkSlotOffset += length;

                    final ByteBuffer slotByteBuffer = bufferPool.byteBuffer(networkSlot);
                    slotByteBuffer.limit(slotByteBuffer.position() + networkSlotOffset);

                    buffer = slotBuffer;
                    offset = 0;
                    length = networkSlotOffset;
                    byteBuffer = slotByteBuffer;
                }
                else
                {
                    writeByteBuffer.clear();
                    buffer.getBytes(offset, writeByteBuffer, length);
                    writeByteBuffer.flip();
                    byteBuffer = writeByteBuffer;
                }

                doNetworkWrite(buffer, offset, length, byteBuffer, traceId);
            }
        }

        private void onApplicationEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = TcpState.closingInitial(state);

            if (networkSlot == NO_SLOT)
            {
                doNetworkShutdownOutput(traceId);
            }
        }

        private void onApplicationAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            doNetworkShutdownOutput(traceId);
        }

        private void onApplicationReset(
            ResetFW reset)
        {
            state = TcpState.closeReply(state);
            CloseHelper.quietClose(network::shutdownInput);

            final long traceId = reset.traceId();

            doCleanup(traceId);
        }

        private void onApplicationWindow(
            WindowFW window)
        {
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudgetId = budgetId;
            replyBudget += credit;
            replyPadding = padding;

            state = TcpState.openReply(state);

            if (replyBudget > replyPadding)
            {
                onNetworkReadable(networkKey);
            }
            else
            {
                networkKey.clear(OP_READ);
            }

            if (replyBudget > replyPadding && !TcpState.replyClosed(state))
            {
                networkKey.register(OP_READ);
                counters.readops.getAsLong();
            }
        }

        private void doApplicationBegin(
            long traceId) throws IOException
        {
            final InetSocketAddress localAddress = (InetSocketAddress) network.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) network.getRemoteAddress();

            router.setThrottle(replyId, this::onApplication);
            doBegin(application, routeId, replyId, traceId, localAddress, remoteAddress);
            state = TcpState.openingReply(state);
        }

        private void doApplicationData(
            DirectBuffer buffer,
            int offset,
            int length)
        {
            final long traceId = supplyTraceId.getAsLong();
            final int reserved = length + replyPadding;

            doData(application, routeId, replyId, traceId, replyBudgetId, reserved, buffer, offset, length);

            replyBudget -= reserved;

            if (replyBudget <= replyPadding)
            {
                networkKey.clear(OP_READ);
            }
        }

        private void doApplicationEnd(
            long traceId)
        {
            doEnd(application, routeId, replyId, traceId);
            state = TcpState.closeReply(state);
        }

        private void doApplicationAbort(
            long traceId)
        {
            doAbort(application, routeId, replyId, traceId);
            state = TcpState.closeReply(state);
        }

        private void doApplicationReset(
            long traceId)
        {
            doReset(application, routeId, initialId, traceId);
            state = TcpState.closeInitial(state);
        }

        private void doApplicationWindow(
            long traceId,
            int credit)
        {
            initialBudget += credit;
            doWindow(application, routeId, initialId, traceId, 0, credit, 0);
        }

        private void doApplicationResetIfNecessary(
            long traceId)
        {
            if (TcpState.initialOpening(state) && !TcpState.initialClosing(state))
            {
                doApplicationReset(traceId);
            }
        }

        private void doApplicationAbortIfNecessary(
            long traceId)
        {
            if (TcpState.replyOpened(state) && !TcpState.replyClosed(state))
            {
                doApplicationAbort(traceId);
            }
        }

        private void doCleanup(
            long traceId)
        {
            doApplicationAbortIfNecessary(traceId);
            doApplicationResetIfNecessary(traceId);

            if (!network.socket().isInputShutdown())
            {
                counters.resetsRead.getAsLong();
            }

            if (!network.socket().isOutputShutdown())
            {
                counters.abortsWritten.getAsLong();
            }

            doCloseNetwork(network);

            cleanupNetworkSlotIfNecessary();
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .affinity(streamId)
                .extension(b -> b.set(tcpBeginEx(localAddress, remoteAddress)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer stream,
        long routeId,
        long streamId,
        long traceId,
        long budgetId,
        int reserved,
        DirectBuffer payload,
        int offset,
        int length)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload, offset, length)
                .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
                               .traceId(traceId)
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long traceId,
        int budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private Flyweight.Builder.Visitor tcpBeginEx(
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        return (buffer, offset, limit) ->
            beginExRW.wrap(buffer, offset, limit)
                     .typeId(tcpTypeId)
                     .localAddress(a -> socketAddress(localAddress, a::ipv4Address, a::ipv6Address))
                     .localPort(localAddress.getPort())
                     .remoteAddress(a -> socketAddress(remoteAddress, a::ipv4Address, a::ipv6Address))
                     .remotePort(remoteAddress.getPort())
                     .build()
                     .sizeof();
    }
}
