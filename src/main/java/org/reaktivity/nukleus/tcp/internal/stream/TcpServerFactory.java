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
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.tcp.internal.TcpNukleus.WRITE_SPIN_COUNT;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.socketAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

public class TcpServerFactory implements StreamFactory
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

    private final TcpBeginExFW.Builder beginExRW = new TcpBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final RouteManager router;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final Long2ObjectHashMap<TcpServer> correlations;
    private final Poller poller;
    private final Runnable onNetworkClosed;

    private final BufferPool bufferPool;
    private final ByteBuffer readByteBuffer;
    private final MutableDirectBuffer readBuffer;
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer writeByteBuffer;
    private final int windowThreshold;
    private final int tcpTypeId;

    final TcpCounters counters;

    public TcpServerFactory(
        TcpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongUnaryOperator supplyReplyId,
        Poller poller,
        TcpCounters counters,
        Runnable onChannelClosed)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.writeByteBuffer = ByteBuffer.allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.poller = requireNonNull(poller);
        this.counters = requireNonNull(counters);
        this.onNetworkClosed = requireNonNull(onChannelClosed);
        this.tcpTypeId = supplyTypeId.applyAsInt(TcpNukleus.NAME);

        final int readBufferSize = Math.min(writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD, bufferPool.slotCapacity());
        this.readByteBuffer = ByteBuffer.allocateDirect(readBufferSize).order(nativeOrder());
        this.readBuffer = new UnsafeBuffer(readByteBuffer);
        this.windowThreshold = (bufferPool.slotCapacity() * config.windowThreshold()) / 100;
        this.correlations = new Long2ObjectHashMap<>();
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
            newStream = newReplyStream(begin, throttle);
        }

        return newStream;
    }

    void onAccepted(
        SocketChannel network,
        InetSocketAddress address,
        LongFunction<InetSocketAddress> lookupAddress)
    {
        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final long routeId = route.correlationId();
            final InetSocketAddress routedAddress = lookupAddress.apply(routeId);
            return compareAddresses(address, routedAddress) == 0;
        };

        final RouteFW route = router.resolveExternal(0L, filter, wrapRoute);

        if (route != null)
        {
            final long routeId = route.correlationId();

            final TcpServer server = new TcpServer(routeId, network);
            correlations.put(server.replyId, server);

            server.onNetworkAccepted();
        }
        else
        {
            doCloseNetwork(network);
        }
    }

    private MessageConsumer newReplyStream(
        BeginFW begin,
        MessageConsumer throttle)
    {
        final long replyId = begin.streamId();
        final TcpServer server = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (server != null)
        {
            newStream = server::onApplication;
        }

        return newStream;
    }

    private void doCloseNetwork(
        SocketChannel network)
    {
        CloseHelper.quietClose(network);
        onNetworkClosed.run();
    }

    private final class TcpServer
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer application;
        private final SocketChannel network;
        private final PollerKey networkKey;
        private final TcpRouteCounters counters;

        private long initialBudgetId;
        private int initialBudget;
        private int initialPadding;

        private int replyBudget;

        private int state;
        private int networkSlot = NO_SLOT;
        private int networkSlotOffset;
        private int bytesFlushed;

        private TcpServer(
            long routeId,
            SocketChannel network)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.application = router.supplyReceiver(initialId);
            this.network = network;
            this.networkKey = poller.doRegister(network, 0, null);
            this.counters = TcpServerFactory.this.counters.supplyRoute(routeId);
        }

        private void onNetworkAccepted()
        {
            try
            {
                networkKey.handler(OP_READ, this::onNetworkReadable);
                networkKey.handler(OP_WRITE, this::onNetworkWritable);

                doApplicationBegin();
            }
            catch (IOException ex)
            {
                doCleanup(supplyTraceId.getAsLong());
            }
        }

        private int onNetworkReadable(
            PollerKey key)
        {
            assert initialBudget > initialPadding;

            final int limit = Math.min(initialBudget - initialPadding, readBuffer.capacity());

            ((Buffer) readByteBuffer).position(0);
            ((Buffer) readByteBuffer).limit(limit);

            try
            {
                final int bytesRead = network.read(readByteBuffer);

                if (bytesRead == -1)
                {
                    key.cancel(OP_READ);
                    CloseHelper.close(network::shutdownInput);

                    doApplicationEnd(supplyTraceId.getAsLong());

                    if (network.socket().isOutputShutdown())
                    {
                        doCloseNetwork(network);
                    }
                }
                else if (bytesRead != 0)
                {
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
            assert networkSlot != NO_SLOT;

            long traceId = supplyTraceId.getAsLong();
            DirectBuffer buffer = bufferPool.buffer(networkSlot);
            ByteBuffer byteBuffer = bufferPool.byteBuffer(networkSlot);
            byteBuffer.limit(byteBuffer.position() + networkSlotOffset);

            return doNetworkWrite(buffer, 0, networkSlotOffset, byteBuffer, traceId);
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

                bytesFlushed += bytesWritten;

                if (bytesWritten < length)
                {
                    if (networkSlot == NO_SLOT)
                    {
                        networkSlot = bufferPool.acquire(replyId);
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
                    if (networkSlot != NO_SLOT)
                    {
                        bufferPool.release(networkSlot);
                        networkSlot = NO_SLOT;
                        networkSlotOffset = 0;

                        networkKey.clear(OP_WRITE);
                    }

                    if (TcpState.replyClosing(state))
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
            if (networkSlot != NO_SLOT)
            {
                bufferPool.release(networkSlot);
                networkSlot = NO_SLOT;
            }

            try
            {
                networkKey.cancel(OP_WRITE);
                network.shutdownOutput();
                state = TcpState.closeReply(state);

                if (network.socket().isInputShutdown())
                {
                    doCloseNetwork(network);
                }
            }
            catch (IOException ex)
            {
                doCleanup(traceId);
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
            final long traceId = begin.traceId();
            final int credit = bufferPool.slotCapacity();

            state = TcpState.openReply(state);
            counters.opensRead.getAsLong();

            doApplicationWindow(traceId, credit);
        }

        private void onApplicationData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                doApplicationReset(traceId);
                doCleanup(traceId, true);
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

            state = TcpState.closingReply(state);

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
            state = TcpState.closeInitial(state);
            CloseHelper.quietClose(network::shutdownInput);

            final boolean abortiveRelease = correlations.containsKey(replyId);
            final long traceId = reset.traceId();

            doCleanup(traceId, abortiveRelease);
        }

        private void onApplicationWindow(
            WindowFW window)
        {
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            initialBudgetId = budgetId;
            initialBudget += credit;
            initialPadding = padding;

            state = TcpState.openInitial(state);

            if (initialBudget > initialPadding)
            {
                onNetworkReadable(networkKey);
            }
            else
            {
                networkKey.clear(OP_READ);
            }

            if (initialBudget > initialPadding && !TcpState.initialClosed(state))
            {
                networkKey.register(OP_READ);
                counters.readops.getAsLong();
            }
        }

        private void doApplicationBegin() throws IOException
        {
            final long traceId = supplyTraceId.getAsLong();
            final InetSocketAddress localAddress = (InetSocketAddress) network.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) network.getRemoteAddress();

            router.setThrottle(initialId, this::onApplication);
            doBegin(application, routeId, initialId, traceId, localAddress, remoteAddress);
            counters.opensWritten.getAsLong();
            state = TcpState.openingInitial(state);
        }

        private void doApplicationData(
            DirectBuffer buffer,
            int offset,
            int length)
        {
            final long traceId = supplyTraceId.getAsLong();
            final int reserved = length + initialPadding;

            doData(application, routeId, initialId, traceId, initialBudgetId, reserved, buffer, offset, length);

            initialBudget -= reserved;

            if (initialBudget <= initialPadding)
            {
                networkKey.clear(OP_READ);
            }
        }

        private void doApplicationEnd(
            long traceId)
        {
            doEnd(application, routeId, initialId, traceId);
            counters.closesWritten.getAsLong();
            state = TcpState.closeInitial(state);
        }

        private void doApplicationAbort(
            long traceId)
        {
            doAbort(application, routeId, initialId, traceId);
            counters.abortsWritten.getAsLong();
            state = TcpState.closeInitial(state);
        }

        private void doApplicationReset(
            long traceId)
        {
            doReset(application, routeId, replyId, traceId);
            counters.resetsWritten.getAsLong();
            state = TcpState.closeReply(state);
        }

        private void doApplicationWindow(
            long traceId,
            int credit)
        {
            replyBudget += credit;
            doWindow(application, routeId, replyId, traceId, 0, credit, 0);
        }

        private void doApplicationResetIfNecessary(
            long traceId)
        {
            if (!TcpState.replyClosing(state))
            {
                if (TcpState.replyOpened(state))
                {
                    assert !correlations.containsKey(replyId);
                    doApplicationReset(traceId);
                }
                else
                {
                    correlations.remove(replyId);
                }
            }
        }

        private void doApplicationAbortIfNecessary(
            long traceId)
        {
            if (TcpState.initialOpened(state) && !TcpState.initialClosed(state))
            {
                doApplicationAbort(traceId);
            }
        }

        private void doCleanup(
            long traceId,
            boolean abortiveRelease)
        {
            if (abortiveRelease)
            {
                try
                {
                    // forces TCP RST
                    network.setOption(StandardSocketOptions.SO_LINGER, 0);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            doCleanup(traceId);
        }

        private void doCleanup(
            long traceId)
        {
            doApplicationAbortIfNecessary(traceId);
            doApplicationResetIfNecessary(traceId);

            doCloseNetwork(network);

            if (networkSlot != NO_SLOT)
            {
                bufferPool.release(networkSlot);
                networkSlot = NO_SLOT;
                networkSlotOffset = 0;
            }
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
