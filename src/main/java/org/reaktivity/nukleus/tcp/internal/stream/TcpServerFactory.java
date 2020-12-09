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

import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.tcp.internal.TcpNukleus.WRITE_SPIN_COUNT;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.proxyAddress;

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
import org.reaktivity.nukleus.tcp.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
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

    private final ProxyBeginExFW.Builder beginExRW = new ProxyBeginExFW.Builder();

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
    private final int proxyTypeId;

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
        this.proxyTypeId = supplyTypeId.applyAsInt("proxy");

        final int readBufferSize = writeBuffer.capacity() - DataFW.FIELD_OFFSET_PAYLOAD;
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

            server.onNetAccepted();
        }
        else
        {
            closeNet(network);
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
            newStream = server::onAppMessage;
        }

        return newStream;
    }

    private void closeNet(
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
        private final MessageConsumer app;
        private final SocketChannel net;
        private final PollerKey key;
        private final TcpRouteCounters counters;

        private long initialBudgetId;
        private int initialBudget;
        private int initialPadding;

        private int replyBudget;

        private int state;
        private int writeSlot = NO_SLOT;
        private int writeSlotOffset;
        private int bytesFlushed;

        private TcpServer(
            long routeId,
            SocketChannel net)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.app = router.supplyReceiver(initialId);
            this.net = net;
            this.key = poller.doRegister(net, 0, null);
            this.counters = TcpServerFactory.this.counters.supplyRoute(routeId);
        }

        private void onNetAccepted()
        {
            try
            {
                key.handler(OP_READ, this::onNetReadable);
                key.handler(OP_WRITE, this::onNetWritable);

                doAppBegin();
            }
            catch (IOException ex)
            {
                cleanup(supplyTraceId.getAsLong());
            }
        }

        private int onNetReadable(
            PollerKey key)
        {
            assert initialBudget > initialPadding;

            final int limit = Math.min(initialBudget - initialPadding, readBuffer.capacity());

            ((Buffer) readByteBuffer).position(0);
            ((Buffer) readByteBuffer).limit(limit);

            try
            {
                final int bytesRead = net.read(readByteBuffer);

                if (bytesRead == -1)
                {
                    key.clear(OP_READ);
                    CloseHelper.close(net::shutdownInput);

                    doAppEnd(supplyTraceId.getAsLong());

                    if (net.socket().isOutputShutdown())
                    {
                        closeNet(net);
                    }
                }
                else if (bytesRead != 0)
                {
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
                assert key == this.key;
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

                bytesFlushed += bytesWritten;

                if (bytesWritten < length)
                {
                    if (writeSlot == NO_SLOT)
                    {
                        writeSlot = bufferPool.acquire(replyId);
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

                        key.register(OP_WRITE);
                        counters.writeops.getAsLong();
                    }
                }
                else
                {
                    cleanupWriteSlot();
                    key.clear(OP_WRITE);

                    if (TcpState.replyClosing(state))
                    {
                        doNetShutdownOutput(traceId);
                    }
                    else if (bytesFlushed >= windowThreshold)
                    {
                        doAppWindow(traceId, bytesFlushed);
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
            cleanupWriteSlot();

            try
            {
                key.clear(OP_WRITE);
                net.shutdownOutput();
                state = TcpState.closeReply(state);

                if (net.socket().isInputShutdown())
                {
                    closeNet(net);
                }
            }
            catch (IOException ex)
            {
                cleanup(traceId);
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
            final long traceId = begin.traceId();
            final int credit = bufferPool.slotCapacity();

            state = TcpState.openReply(state);
            counters.opensRead.getAsLong();

            doAppWindow(traceId, credit);
        }

        private void onAppData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                doAppReset(traceId);
                cleanup(traceId, true);
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
            final long traceId = end.traceId();

            state = TcpState.closingReply(state);

            if (writeSlot == NO_SLOT)
            {
                doNetShutdownOutput(traceId);
            }
        }

        private void onAppAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            doNetShutdownOutput(traceId);
        }

        private void onAppReset(
            ResetFW reset)
        {
            state = TcpState.closeInitial(state);
            CloseHelper.quietClose(net::shutdownInput);

            final boolean abortiveRelease = correlations.containsKey(replyId);
            final long traceId = reset.traceId();

            cleanup(traceId, abortiveRelease);
        }

        private void onAppWindow(
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
                onNetReadable(key);
            }
            else
            {
                key.clear(OP_READ);
            }

            if (initialBudget > initialPadding && !TcpState.initialClosed(state))
            {
                key.register(OP_READ);
                counters.readops.getAsLong();
            }
        }

        private void doAppBegin() throws IOException
        {
            final long traceId = supplyTraceId.getAsLong();
            final InetSocketAddress localAddress = (InetSocketAddress) net.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) net.getRemoteAddress();

            router.setThrottle(initialId, this::onAppMessage);
            doBegin(app, routeId, initialId, traceId, localAddress, remoteAddress);
            counters.opensWritten.getAsLong();
            state = TcpState.openingInitial(state);
        }

        private void doAppData(
            DirectBuffer buffer,
            int offset,
            int length)
        {
            final long traceId = supplyTraceId.getAsLong();
            final int reserved = length + initialPadding;

            doData(app, routeId, initialId, traceId, initialBudgetId, reserved, buffer, offset, length);

            initialBudget -= reserved;

            if (initialBudget <= initialPadding)
            {
                key.clear(OP_READ);
            }
        }

        private void doAppEnd(
            long traceId)
        {
            doEnd(app, routeId, initialId, traceId);
            counters.closesWritten.getAsLong();
            state = TcpState.closeInitial(state);
        }

        private void doAppWindow(
            long traceId,
            int credit)
        {
            replyBudget += credit;
            doWindow(app, routeId, replyId, traceId, 0, credit, 0);
        }

        private void doAppReset(
            long traceId)
        {
            if (!TcpState.replyClosing(state))
            {
                if (TcpState.replyOpened(state))
                {
                    assert !correlations.containsKey(replyId);

                    doReset(app, routeId, replyId, traceId);
                    counters.resetsWritten.getAsLong();
                    state = TcpState.closeReply(state);
                }
                else
                {
                    correlations.remove(replyId);
                }
            }
        }

        private void doAppAbort(
            long traceId)
        {
            if (TcpState.initialOpened(state) && !TcpState.initialClosed(state))
            {
                doAbort(app, routeId, initialId, traceId);
                counters.abortsWritten.getAsLong();
                state = TcpState.closeInitial(state);
            }
        }

        private void cleanup(
            long traceId,
            boolean abortiveRelease)
        {
            if (abortiveRelease)
            {
                try
                {
                    // forces TCP RST
                    net.setOption(StandardSocketOptions.SO_LINGER, 0);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            cleanup(traceId);
        }

        private void cleanup(
            long traceId)
        {
            doAppAbort(traceId);
            doAppReset(traceId);

            cleanupWriteSlot();

            closeNet(net);
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
                .extension(b -> b.set(proxyBeginEx(remoteAddress, localAddress)))
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

    private Flyweight.Builder.Visitor proxyBeginEx(
        InetSocketAddress source,
        InetSocketAddress destination)
    {
        return (buffer, offset, limit) ->
            beginExRW.wrap(buffer, offset, limit)
                     .typeId(proxyTypeId)
                     .address(a -> proxyAddress(a, source, destination))
                     .build()
                     .sizeof();
    }
}
