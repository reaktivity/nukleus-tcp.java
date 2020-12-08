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
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.proxyAddress;

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
import java.util.function.Function;
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
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressFW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInet4FW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInet6FW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInetFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.tcp.internal.util.Cidr;

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

    private final ProxyBeginExFW beginExRO = new ProxyBeginExFW();
    private final ProxyBeginExFW.Builder beginExRW = new ProxyBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final BufferPool bufferPool;
    private final Poller poller;
    private final RouteManager router;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer writeByteBuffer;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final Function<String, InetAddress[]> resolveHost;
    private final int proxyTypeId;
    private final Map<String, Predicate<? super InetAddress>> targetToCidrMatch;
    private final TcpCounters counters;
    private final int windowThreshold;
    private final boolean keepalive;
    private final int initialMax;

    public TcpClientFactory(
        TcpConfiguration config,
        RouteManager router,
        Poller poller,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Function<String, InetAddress[]> resolveHost,
        TcpCounters counters)
    {
        this.router = requireNonNull(router);
        this.poller = poller;
        this.writeBuffer = requireNonNull(writeBuffer);
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.resolveHost = requireNonNull(resolveHost);
        this.proxyTypeId = supplyTypeId.applyAsInt("proxy");

        final int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;
        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
        this.targetToCidrMatch = new HashMap<>();

        this.counters = counters;
        this.initialMax = bufferPool.slotCapacity();
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
            client.doNetConnect(remoteAddress);
            newStream = client::onAppMessage;
        }

        return newStream;
    }

    private InetSocketAddress resolveRemoteAddressExt(
        OctetsFW extension,
        String targetName,
        long targetRef)
    {
        final ProxyBeginExFW beginEx = extension.get(beginExRO::wrap);
        final ProxyAddressFW address = beginEx.address();

        InetSocketAddress resolved = null;
        try
        {
            Predicate<? super InetAddress> matchSubnet = extensionMatcher(targetName);

            switch (address.kind())
            {
            case INET:
                ProxyAddressInetFW addressInet = address.inet();
                resolved = Arrays
                        .stream(resolveHost.apply(addressInet.destination().asString()))
                        .filter(matchSubnet)
                        .findFirst()
                        .map(a -> new InetSocketAddress(a, addressInet.destinationPort()))
                        .orElse(null);
                break;
            case INET4:
                ProxyAddressInet4FW addressInet4 = address.inet4();
                OctetsFW destinationInet4 = addressInet4.destination();
                int destinationPortInet4 = addressInet4.destinationPort();
                byte[] ipv4 = new byte[4];
                destinationInet4.buffer().getBytes(destinationInet4.offset(), ipv4);
                resolved = Optional
                        .of(InetAddress.getByAddress(ipv4))
                        .filter(matchSubnet)
                        .map(a -> new InetSocketAddress(a, destinationPortInet4))
                        .orElse(null);
                break;
            case INET6:
                ProxyAddressInet6FW addressInet6 = address.inet6();
                OctetsFW destinationInet6 = addressInet6.destination();
                int destinationPortInet6 = addressInet6.destinationPort();
                byte[] ipv6 = new byte[16];
                destinationInet6.buffer().getBytes(destinationInet6.offset(), ipv6);
                resolved = Optional
                        .of(InetAddress.getByAddress(ipv6))
                        .filter(matchSubnet)
                        .map(a -> new InetSocketAddress(a, destinationPortInet6))
                        .orElse(null);
                break;
            default:
                throw new RuntimeException("Unexpected address kind");
            }
        }
        catch (UnknownHostException ignore)
        {
           // NOOP
        }

        return resolved;
    }

    private Predicate<? super InetAddress> extensionMatcher(
        String targetName) throws UnknownHostException
    {
        Predicate<? super InetAddress> result;
        if (targetName.contains("/"))
        {
            result = targetToCidrMatch.computeIfAbsent(targetName, this::inetMatchesCidr);
        }
        else
        {
            InetAddress.getByName(targetName);
            result = targetToCidrMatch.computeIfAbsent(targetName, this::inetMatchesInet);
        }
        return result;
    }

    private Predicate<InetAddress> inetMatchesCidr(
        String targetName)
    {
        return new Cidr(targetName)::matches;
    }

    private Predicate<InetAddress> inetMatchesInet(
        String targetName)
    {
        try
        {
            return InetAddress.getByName(targetName)::equals;
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

    private void closeNet(
        SocketChannel network)
    {
        CloseHelper.quietClose(network);
    }

    private final class TcpClient
    {
        private final MessageConsumer app;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final SocketChannel net;
        private final TcpRouteCounters counters;

        private PollerKey networkKey;

        private long replySeq;
        private long replyAck;
        private long replyBudgetId;
        private int replyMax;
        private int replyPad;

        private long initialSeq;
        private long initialAck;

        private int state;
        private int writeSlot = NO_SLOT;
        private int writeSlotOffset;
        private int bytesFlushed;

        private TcpClient(
            MessageConsumer app,
            long routeId,
            long initialId,
            SocketChannel net,
            TcpRouteCounters counters)
        {
            this.app = app;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.net = net;
            this.counters = counters;
        }

        private void doNetConnect(
            InetSocketAddress remoteAddress)
        {
            try
            {
                state = TcpState.openingInitial(state);
                counters.opensWritten.getAsLong();
                net.setOption(SO_KEEPALIVE, keepalive);

                if (net.connect(remoteAddress))
                {
                    onNetConnected();
                }
                else
                {
                    networkKey = poller.doRegister(net, OP_CONNECT, this::onNetConnect);
                }
            }
            catch (UnresolvedAddressException | IOException ex)
            {
                onNetRejected();
            }
        }

        private int onNetConnect(
            PollerKey key)
        {
            try
            {
                key.clear(OP_CONNECT);
                net.finishConnect();
                onNetConnected();
            }
            catch (UnresolvedAddressException | IOException ex)
            {
                onNetRejected();
            }

            return 1;
        }

        private void onNetConnected()
        {
            final long traceId = supplyTraceId.getAsLong();

            state = TcpState.openInitial(state);
            counters.opensRead.getAsLong();

            try
            {
                networkKey.handler(OP_READ, this::onNetReadable);
                networkKey.handler(OP_WRITE, this::onNetWritable);

                doAppBegin(traceId);
                doAppWindow(traceId);
            }
            catch (IOException ex)
            {
                cleanup(traceId);
            }
        }

        private void onNetRejected()
        {
            final long traceId = supplyTraceId.getAsLong();

            counters.resetsRead.getAsLong();

            cleanup(traceId);
        }

        private int onNetReadable(
            PollerKey key)
        {
            final int replyBudget = (int) Math.max(replyMax - (replySeq - replyAck), 0L);

            assert replyBudget > replyPad;

            final int limit = Math.min(replyBudget - replyPad, readBuffer.capacity());

            ((Buffer) readByteBuffer).position(0);
            ((Buffer) readByteBuffer).limit(limit);

            try
            {
                final int bytesRead = net.read(readByteBuffer);

                if (bytesRead == -1)
                {
                    key.clear(OP_READ);
                    CloseHelper.close(net::shutdownInput);
                    counters.closesRead.getAsLong();

                    doAppEnd(supplyTraceId.getAsLong());

                    if (net.socket().isOutputShutdown())
                    {
                        closeNet(net);
                    }
                }
                else if (bytesRead != 0)
                {
                    counters.bytesRead.accept(bytesRead);
                    doAppData(readBuffer, 0, bytesRead);
                }
            }
            catch (IOException ex)
            {
                cleanup(supplyTraceId.getAsLong());
            }

            return 1;
        }

        private int onNetWritable(
            PollerKey key)
        {
            if (writeSlot == NO_SLOT)
            {
                counters.writeopsNoSlot.getAsLong();
                assert key == networkKey;
                return 0;
            }
            else
            {
                assert writeSlot != NO_SLOT;

                long traceId = supplyTraceId.getAsLong();
                DirectBuffer buffer = bufferPool.buffer(writeSlot);
                ByteBuffer byteBuffer = bufferPool.byteBuffer(writeSlot);
                byteBuffer.limit(byteBuffer.position() + writeSlotOffset);

                return doNetWrite(buffer, 0, writeSlotOffset, byteBuffer, traceId);
            }
        }

        private int doNetWrite(
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
                    bytesWritten = net.write(byteBuffer);
                }

                counters.bytesWritten.accept(bytesWritten);

                bytesFlushed += bytesWritten;

                if (bytesWritten < length)
                {
                    if (writeSlot == NO_SLOT)
                    {
                        writeSlot = bufferPool.acquire(initialId);
                    }

                    if (writeSlot == NO_SLOT)
                    {
                        counters.overflows.getAsLong();
                        doAppReset(traceId);
                        cleanup(traceId);
                    }
                    else
                    {
                        final MutableDirectBuffer slotBuffer = bufferPool.buffer(writeSlot);
                        slotBuffer.putBytes(0, buffer, offset + bytesWritten, length - bytesWritten);
                        writeSlotOffset = length - bytesWritten;

                        networkKey.register(OP_WRITE);
                        counters.writeops.getAsLong();
                    }
                }
                else
                {
                    cleanupWriteSlot();
                    networkKey.clear(OP_WRITE);

                    if (TcpState.initialClosing(state))
                    {
                        doNetShutdownOutput(traceId);
                    }
                    else if (bytesFlushed >= windowThreshold)
                    {
                        initialAck += bytesFlushed;
                        doAppWindow(traceId);
                        bytesFlushed = 0;
                    }
                }
            }
            catch (IOException ex)
            {
                cleanup(traceId);
            }

            return bytesWritten;
        }

        private void doNetShutdownOutput(
            long traceId)
        {
            state = TcpState.closeInitial(state);

            cleanupWriteSlot();

            try
            {
                if (net.isConnectionPending())
                {
                    networkKey.clear(OP_CONNECT);
                    closeNet(net);
                    counters.closesWritten.getAsLong();
                }
                else
                {
                    networkKey.clear(OP_WRITE);
                    net.shutdownOutput();
                    counters.closesWritten.getAsLong();

                    if (net.socket().isInputShutdown())
                    {
                        closeNet(net);
                    }
                }
            }
            catch (IOException ex)
            {
                cleanup(traceId);
            }
        }

        private void cleanupWriteSlot()
        {
            if (writeSlot != NO_SLOT)
            {
                bufferPool.release(writeSlot);
                writeSlot = NO_SLOT;
                writeSlotOffset = 0;
            }
        }

        private void onAppMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onAppBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onAppData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onAppEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAppAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onAppReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onAppWindow(window);
                break;
            }
        }

        private void onAppBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;
            assert acknowledge >= initialAck;

            initialSeq = sequence;
            initialAck = acknowledge;

            assert initialAck <= initialSeq;

            assert TcpState.initialOpening(state);
        }

        private void onAppData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final int reserved = data.reserved();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence + reserved;

            assert initialAck <= initialSeq;

            if (initialSeq > initialAck + initialMax)
            {
                doAppReset(traceId);
                cleanup(traceId);
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

                if (writeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(writeSlot);
                    slotBuffer.putBytes(writeSlotOffset, buffer, offset, length);
                    writeSlotOffset += length;

                    final ByteBuffer slotByteBuffer = bufferPool.byteBuffer(writeSlot);
                    slotByteBuffer.limit(slotByteBuffer.position() + writeSlotOffset);

                    buffer = slotBuffer;
                    offset = 0;
                    length = writeSlotOffset;
                    byteBuffer = slotByteBuffer;
                }
                else
                {
                    writeByteBuffer.clear();
                    buffer.getBytes(offset, writeByteBuffer, length);
                    writeByteBuffer.flip();
                    byteBuffer = writeByteBuffer;
                }

                doNetWrite(buffer, offset, length, byteBuffer, traceId);
            }
        }

        private void onAppEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final long traceId = end.traceId();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            state = TcpState.closingInitial(state);

            if (writeSlot == NO_SLOT)
            {
                doNetShutdownOutput(traceId);
            }
        }

        private void onAppAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final long traceId = abort.traceId();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            doNetShutdownOutput(traceId);
        }

        private void onAppReset(
            ResetFW reset)
        {
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();

            assert acknowledge <= sequence;
            assert acknowledge >= replyAck;

            replyAck = acknowledge;

            assert replyAck <= replySeq;

            state = TcpState.closeReply(state);
            CloseHelper.quietClose(net::shutdownInput);

            cleanup(traceId);
        }

        private void onAppWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final long budgetId = window.budgetId();
            final int maximum = window.maximum();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert acknowledge >= replyAck;
            assert maximum >= replyMax;

            replyAck = acknowledge;
            replyMax = maximum;
            replyBudgetId = budgetId;
            replyPad = padding;

            assert replyAck <= replySeq;

            state = TcpState.openReply(state);

            if (replySeq + replyPad < replyAck + replyMax)
            {
                onNetReadable(networkKey);
            }
            else
            {
                networkKey.clear(OP_READ);
            }

            if (replySeq + replyPad < replyAck + replyMax && !TcpState.replyClosed(state))
            {
                networkKey.register(OP_READ);
                counters.readops.getAsLong();
            }
        }

        private void doAppBegin(
            long traceId) throws IOException
        {
            final InetSocketAddress localAddress = (InetSocketAddress) net.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) net.getRemoteAddress();

            router.setThrottle(replyId, this::onAppMessage);
            doBegin(app, routeId, replyId, replySeq, replyAck, replyMax, traceId, localAddress, remoteAddress);
            state = TcpState.openingReply(state);
        }

        private void doAppData(
            DirectBuffer buffer,
            int offset,
            int length)
        {
            final long traceId = supplyTraceId.getAsLong();
            final int reserved = length + replyPad;

            doData(app, routeId, replyId, replySeq, replyAck, replyMax, traceId, replyBudgetId,
                    reserved, buffer, offset, length);

            replySeq += reserved;

            if (replySeq + replyPad >= replyAck + replyMax)
            {
                networkKey.clear(OP_READ);
            }
        }

        private void doAppEnd(
            long traceId)
        {
            doEnd(app, routeId, replyId, replySeq, replyAck, replyMax, traceId);
            state = TcpState.closeReply(state);
        }

        private void doAppWindow(
            long traceId)
        {
            doWindow(app, routeId, initialId, initialSeq, initialAck, initialMax, traceId, 0, 0);
        }

        private void doAppReset(
            long traceId)
        {
            if (TcpState.initialOpening(state) && !TcpState.initialClosing(state))
            {
                doReset(app, routeId, initialId, initialSeq, initialAck, initialMax, traceId);
                state = TcpState.closeInitial(state);
            }
        }

        private void doAppAbort(
            long traceId)
        {
            if (TcpState.replyOpened(state) && !TcpState.replyClosed(state))
            {
                doAbort(app, routeId, replyId, replySeq, replyAck, replyMax, traceId);
                state = TcpState.closeReply(state);
            }
        }

        private void cleanup(
            long traceId)
        {
            doAppAbort(traceId);
            doAppReset(traceId);

            if (!net.socket().isInputShutdown())
            {
                counters.resetsRead.getAsLong();
            }

            if (!net.socket().isOutputShutdown())
            {
                counters.abortsWritten.getAsLong();
            }

            closeNet(net);

            cleanupWriteSlot();
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .affinity(streamId)
                .extension(b -> b.set(proxyBeginEx(localAddress, remoteAddress)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer stream,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
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
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
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
        long sequence,
        long acknowledge,
        int maximum,
        long traceId)
    {
        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(traceId)
               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId)
    {
        AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                 .routeId(routeId)
                 .streamId(streamId)
                 .sequence(sequence)
                 .acknowledge(acknowledge)
                 .maximum(maximum)
                 .traceId(traceId)
                 .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId)
    {
        ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                 .routeId(routeId)
                 .streamId(streamId)
                 .sequence(sequence)
                 .acknowledge(acknowledge)
                 .maximum(maximum)
                 .traceId(traceId)
                 .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        int budgetId,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .budgetId(budgetId)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private Flyweight.Builder.Visitor proxyBeginEx(
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        return (buffer, offset, limit) ->
            beginExRW.wrap(buffer, offset, limit)
                     .typeId(proxyTypeId)
                     .address(a -> proxyAddress(a, localAddress, remoteAddress))
                     .build()
                     .sizeof();
    }
}
