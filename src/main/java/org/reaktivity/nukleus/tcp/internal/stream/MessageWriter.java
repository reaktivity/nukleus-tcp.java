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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.socketAddress;

import java.net.InetSocketAddress;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.TcpNukleusFactorySpi;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

final class MessageWriter
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(TcpNukleusFactorySpi.NAME.getBytes(UTF_8));

    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final ResetFW resetRO = new ResetFW();
    final WindowFW windowRO = new WindowFW();

    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();


    private final TcpBeginExFW.Builder beginExRW = new TcpBeginExFW.Builder();

    private final MutableDirectBuffer writeBuffer;

    MessageWriter(MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
    }

    public void doTcpAbort(
            MessageConsumer stream,
            long streamId)
    {
        AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(b -> b.set((buf, off, len) -> 0))
                .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    public void doTcpBegin(
        MessageConsumer stream,
        long streamId,
        long referenceId,
        long correlationId,
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                .sourceRef(referenceId)
                .correlationId(correlationId)
                .extension(b -> b.set(visitBeginEx(localAddress, remoteAddress)))
                .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doTcpData(
        MessageConsumer stream,
        long streamId,
        long groupId,
        int padding,
        DirectBuffer payload,
        int offset,
        int length)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .groupId(groupId)
                .padding(padding)
                .payload(payload, offset, length)
                .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doTcpEnd(
        MessageConsumer stream,
        long streamId)
    {
        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(b -> b.set((buf, off, len) -> 0))
                .build();

       stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding,
        final int groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
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

}
