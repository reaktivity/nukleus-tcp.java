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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.socketAddress;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.TcpNukleusFactorySpi;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.ListFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.AckFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.TransferFW;

final class StreamHelper
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(TcpNukleusFactorySpi.NAME.getBytes(UTF_8));

    final BeginFW beginRO = new BeginFW();
    final TransferFW transferRO = new TransferFW();
    final AckFW ackRO = new AckFW();

    private final MutableDirectBuffer memoryBufferRW = new UnsafeBuffer(new byte[0]);
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder transferRW = new TransferFW.Builder();
    private final AckFW.Builder ackRW = new AckFW.Builder();

    private final TcpBeginExFW.Builder beginExRW = new TcpBeginExFW.Builder();

    private final MutableDirectBuffer backlogRW = new UnsafeBuffer(new byte[0]);
    private final ListFW<RegionFW> regionsRO = new ListFW<>(new RegionFW());
    private final ListFW.Builder<RegionFW.Builder, RegionFW> regionsRW =
            new ListFW.Builder<>(new RegionFW.Builder(), new RegionFW());

    private final MutableInteger integerRW = new MutableInteger();

    private final MemoryManager memory;
    private final int transferCapacity;
    private final int pendingRegionsCapacity;

    private final DirectBufferBuilder directBufferBuilder;
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer readByteBuffer;

    StreamHelper(
        MemoryManager memory,
        DirectBufferBuilder directBufferBuilder,
        MutableDirectBuffer writeBuffer,
        int transferCapacity,
        int pendingRegionsCapacity)
    {
        this.memory = memory;
        this.directBufferBuilder = directBufferBuilder;
        this.writeBuffer = writeBuffer;
        this.readByteBuffer = allocateDirect(writeBuffer.capacity()).order(nativeOrder());
        this.transferCapacity = transferCapacity;
        this.pendingRegionsCapacity = pendingRegionsCapacity;
    }

    public int memoryOffset(
        long memoryIndex)
    {
        return (int) (memoryIndex & (transferCapacity - 1));
    }

    public DirectBuffer toDirectBuffer(
        ListFW<RegionFW> regions,
        int bytesWritten)
    {
        final MutableInteger written = integerRW;
        written.value = bytesWritten;
        regions.forEach(r ->
        {
            written.value -= r.length();

            if (written.value < 0)
            {
                final int offset = Math.max(r.length() + written.value, 0);
                final int length = r.length() - offset;
                final long address = memory.resolve(r.address() + offset);

                directBufferBuilder.wrap(address, length);
            }
        });

        return directBufferBuilder.build();
    }

    public ListFW<RegionFW> wrapBacklogRegions(
        long address)
    {
        assert (address != -1);

        MutableDirectBuffer backlog = backlogRW;
        backlog.wrap(memory.resolve(address), pendingRegionsCapacity);

        return regionsRO.wrap(backlog, 0, backlog.capacity());
    }

    public ListFW<RegionFW> appendBacklogRegions(
        long address,
        ListFW<RegionFW> regions)
    {
        if (address != -1)
        {
            MutableDirectBuffer backlog = backlogRW;
            backlog.wrap(memory.resolve(address), pendingRegionsCapacity);
            regionsRW.wrap(backlog, 0, backlog.capacity());
            regionsRO.wrap(backlog, 0, backlog.capacity())
                     .forEach(this::appendRegion);
            regions.forEach(this::appendRegion);
            regions = regionsRW.build();
        }

        return regions;
    }

    public ListFW<RegionFW> setBacklogRegions(
        long address,
        ListFW<RegionFW> regions)
    {
        if (address != -1)
        {
            MutableDirectBuffer backlog = backlogRW;
            backlog.wrap(memory.resolve(address), pendingRegionsCapacity);
            backlog.putBytes(0, regions.buffer(), regions.offset(), regions.sizeof());
            regions = regionsRO.wrap(backlog, 0, backlog.capacity());
        }

        return regions;
    }

    private void appendRegion(
        RegionFW region)
    {
        regionsRW.item(r -> r.address(region.address())
                             .length(region.length())
                             .streamId(region.streamId()));
    }

    public int readMemoryMask()
    {
        return transferCapacity - 1;
    }

    public long acquireWriteMemory(
        long address)
    {
        return address == -1L ? memory.acquire(pendingRegionsCapacity) : address;
    }

    public long releaseWriteMemory(
        long address)
    {
        if (address != -1L)
        {
            memory.release(address, pendingRegionsCapacity);
        }
        return -1L;
    }

    public long acquireReadMemory()
    {
        return memory.acquire(transferCapacity);
    }

    public long releaseReadMemory(
        long address)
    {
        if (address != -1L)
        {
            memory.release(address, transferCapacity);
        }
        return -1L;
    }

    public MutableDirectBuffer wrapMemory(
        long memoryAddress)
    {
        final MutableDirectBuffer memoryBuffer = memoryBufferRW;
        memoryBuffer.wrap(memory.resolve(memoryAddress), transferCapacity);
        return memoryBuffer;
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

    public void doTcpTransfer(
        MessageConsumer stream,
        long streamId,
        int flags,
        Consumer<ListFW.Builder<RegionFW.Builder, RegionFW>> regions)
    {
        TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .flags(flags)
                .regions(regions)
                .build();

        stream.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    public void doTcpTransfer(
        MessageConsumer stream,
        long streamId,
        int flags)
    {
        TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .flags(flags)
                .build();

        stream.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    public void doAck(
        MessageConsumer throttle,
        long throttleId,
        int flags,
        ListFW<RegionFW> regions)
    {
        AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(flags)
                .regions(b -> regions.forEach(r -> b.item(i -> i.address(r.address())
                                                                .length(r.length())
                                                                .streamId(r.streamId()))))
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
    }

    void doAck(
        MessageConsumer throttle,
        long throttleId,
        int flags)
    {
        AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(flags)
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
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

    public ByteBuffer readByteBuffer(
        int position)
    {
        final int memoryAvailable = Math.min(readByteBuffer.capacity() - position, transferCapacity);

        readByteBuffer.position(0);
        readByteBuffer.limit(memoryAvailable);

        return readByteBuffer;
    }
}
