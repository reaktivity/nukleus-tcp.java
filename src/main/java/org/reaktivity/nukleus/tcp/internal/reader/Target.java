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
package org.reaktivity.nukleus.tcp.internal.reader;

import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.socketAddress;

import java.net.InetSocketAddress;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;

public final class Target implements Nukleus
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder tcpDataRW = new DataFW.Builder();
    private final EndFW.Builder tcpEndRW = new EndFW.Builder();

    private final TcpBeginExFW.Builder beginExRW = new TcpBeginExFW.Builder();

    private final String name;
    private final StreamsLayout layout;
    private final AtomicBuffer writeBuffer;

    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final Long2ObjectHashMap<MessageHandler> throttles;

    public Target(
        String name,
        StreamsLayout layout,
        AtomicBuffer writeBuffer)
    {
        this.name = name;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.throttles = new Long2ObjectHashMap<>();
    }

    @Override
    public int process()
    {
        return throttleBuffer.read(this::handleRead);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), name);
    }

    public void addThrottle(
        long streamId,
        MessageHandler throttle)
    {
        throttles.put(streamId, throttle);
    }

    public void removeThrottle(
        long streamId)
    {
        throttles.remove(streamId);
    }

    public void doTcpBegin(
        long streamId,
        long referenceId,
        long correlationId,
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .referenceId(referenceId)
                .streamId(streamId)
                .correlationId(correlationId)
                .extension(b -> b.set(visitBeginEx(localAddress, remoteAddress)))
                .build();

        streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doTcpData(
        long streamId,
        DirectBuffer payload,
        int offset,
        int length)
    {
        DataFW tcpData = tcpDataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .payload(p -> p.set(payload, offset, length))
                .build();

        streamsBuffer.write(tcpData.typeId(), tcpData.buffer(), tcpData.offset(), tcpData.sizeof());
    }

    public void doTcpEnd(
        long streamId)
    {
        EndFW tcpEnd = tcpEndRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(b -> b.set((buf, off, len) -> 0))
                .build();

        streamsBuffer.write(tcpEnd.typeId(), tcpEnd.buffer(), tcpEnd.offset(), tcpEnd.sizeof());
    }

    private Flyweight.Builder.Visitor visitBeginEx(
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        return (buffer, offset, limit) ->
            beginExRW.wrap(buffer, offset, limit)
                     .localAddress(a -> socketAddress(localAddress, a::ipv4Address, a::ipv6Address))
                     .localPort(localAddress.getPort())
                     .remoteAddress(a -> socketAddress(remoteAddress, a::ipv4Address, a::ipv6Address))
                     .remotePort(remoteAddress.getPort())
                     .build()
                     .sizeof();
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();
        final MessageHandler throttle = throttles.get(streamId);

        if (throttle != null)
        {
            throttle.onMessage(msgTypeId, buffer, index, length);
        }
    }
}
