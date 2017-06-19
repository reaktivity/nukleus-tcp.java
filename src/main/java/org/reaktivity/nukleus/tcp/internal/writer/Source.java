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
package org.reaktivity.nukleus.tcp.internal.writer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.WINDOW_SIZE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.tcp.internal.TcpNukleus;
import org.reaktivity.nukleus.tcp.internal.connector.Connector;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;
import org.reaktivity.nukleus.tcp.internal.router.RouteKind;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory;

public final class Source implements Nukleus
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(TcpNukleus.NAME.getBytes(UTF_8));

    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private final String partitionName;
    private final Connector connector;
    private final LongFunction<List<Route>> lookupRoutes;
    private final LongFunction<Correlation> resolveCorrelation;
    private final Function<String, Target> supplyTarget;
    private final StreamsLayout layout;
    private final AtomicBuffer writeBuffer;
    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final StreamFactory streamFactory;
    private final Long2ObjectHashMap<MessageHandler> streams;
    private final MessageHandler readHandler;

    Source(
        String partitionName,
        Connector connector,
        LongFunction<List<Route>> lookupRoutes,
        LongFunction<Correlation> resolveCorrelation,
        Function<String, Target> supplyTarget,
        Long2ObjectHashMap<MessageHandler> streams,
        StreamsLayout layout,
        AtomicBuffer writeBuffer,
        int maximumStreamsCount,
        LongSupplier incrementOverflow)
    {
        this.partitionName = partitionName;
        this.connector = connector;
        this.lookupRoutes = lookupRoutes;
        this.resolveCorrelation = resolveCorrelation;
        this.supplyTarget = supplyTarget;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.streamFactory = new StreamFactory(this, WINDOW_SIZE.intValue(), maximumStreamsCount, incrementOverflow);
        this.streams = streams;
        this.readHandler = this::handleRead;
    }

    @Override
    public int process()
    {
        return streamsBuffer.read(readHandler);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return partitionName;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), partitionName);
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();
        final MessageHandler stream = streams.get(streamId);

        if (stream != null)
        {
            stream.onMessage(msgTypeId, buffer, index, length);
        }
        else if (msgTypeId == BeginFW.TYPE_ID)
        {
            handleBegin(buffer, index, length);
        }
        else
        {
            doReset(streamId);
        }
    }

    private void handleBegin(
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        beginRO.wrap(buffer, index, index + length);

        final long streamId = beginRO.streamId();
        final long sourceRef = beginRO.sourceRef();
        final long correlationId = beginRO.correlationId();

        if (sourceRef == 0L)
        {
            handleBeginServerReply(buffer, index, length, streamId, correlationId);
        }
        else
        {
            switch (RouteKind.match(sourceRef))
            {
            case OUTPUT_NEW:
                handleBeginClient(streamId, sourceRef, correlationId);
                break;
            default:
                doReset(streamId);
                break;
            }
        }
    }

    private void handleBeginServerReply(
        MutableDirectBuffer buffer,
        int index,
        int length,
        final long streamId,
        final long correlationId)
    {
        final Correlation correlation = resolveCorrelation.apply(correlationId);

        if (correlation != null)
        {
            final SocketChannel channel = correlation.channel();

            final String targetName = correlation.source();
            final Target target = supplyTarget.apply(targetName);
            final MessageHandler newStream = streamFactory.newStream(streamId, target, channel);

            streams.put(streamId, newStream);

            newStream.onMessage(BeginFW.TYPE_ID, buffer, index, length);
        }
        else
        {
            doReset(streamId);
        }
    }

    private void handleBeginClient(
        final long streamId,
        final long referenceId,
        final long correlationId)
    {
        final List<Route> routes = lookupRoutes.apply(referenceId);

        final Optional<Route> optional = routes.stream().findFirst();

        if (optional.isPresent())
        {
            final Route route = optional.get();
            final Target target = route.target();
            final long targetRef = route.targetRef();
            final SocketChannel channel = newSocketChannel();

            final MessageHandler newStream = streamFactory.newStream(streamId, target, channel);

            streams.put(streamId, newStream);

            final String targetName = route.target().name();
            final InetSocketAddress remoteAddress = route.address();

            connector.doConnect(
                    partitionName, referenceId, streamId, correlationId, targetName, targetRef, channel, remoteAddress);
        }
        else
        {
            doReset(streamId);
        }
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

    public void onConnected(
        long sourceId,
        long sourceRef,
        Target target,
        SocketChannel channel,
        long correlationId)
    {
        final MessageHandler newStream = streamFactory.newStream(sourceId, target, channel);

        streams.put(sourceId, newStream);

        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .streamId(sourceId)
            .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
            .sourceRef(sourceRef)
            .correlationId(correlationId)
            .build();

        newStream.onMessage(begin.typeId(), writeBuffer, begin.offset(), begin.sizeof());
    }

    public void doWindow(
        final long streamId,
        final int windowBytes,
        final int windowFrames)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .update(windowBytes)
                .frames(windowFrames)
                .build();

        throttleBuffer.write(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    public void doReset(
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        throttleBuffer.write(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    public void replaceStream(
        long streamId,
        MessageHandler handler)
    {
        streams.put(streamId, handler);
    }

    public void removeStream(
        long streamId)
    {
        streams.remove(streamId);
    }
}
