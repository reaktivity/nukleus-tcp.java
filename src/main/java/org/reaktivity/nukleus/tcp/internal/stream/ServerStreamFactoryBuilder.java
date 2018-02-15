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

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;

public class ServerStreamFactoryBuilder implements StreamFactoryBuilder
{
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final TcpConfiguration configuration;
    private final Acceptor acceptor;
    private final Poller poller;
    private final Long2ObjectHashMap<Correlation> correlations;

    private final Long2ObjectHashMap<LongSupplier> framesWrittenByteRouteId;
    private final Long2ObjectHashMap<LongSupplier> framesReadByteRouteId;
    private final Long2ObjectHashMap<LongConsumer> bytesWrittenByteRouteId;
    private final Long2ObjectHashMap<LongConsumer> bytesReadByteRouteId;

    private LongSupplier incrementOverflow;
    private RouteManager router;
    private LongSupplier supplyStreamId;
    private MemoryManager memory;
    private LongSupplier supplyCorrelationId;
    private MutableDirectBuffer writeBuffer;
    private Supplier<DirectBufferBuilder> supplyDirectBufferBuilder;
    private Function<String, LongSupplier> supplyCounter;
    private Function<String, LongConsumer> supplyAccumulator;

    private Function<RouteFW, LongSupplier> supplyWriteFrameCounter;
    private Function<RouteFW, LongSupplier> supplyReadFrameCounter;
    private Function<RouteFW, LongConsumer> supplyWriteBytesAccumulator;
    private Function<RouteFW, LongConsumer> supplyReadBytesAccumulator;

    public ServerStreamFactoryBuilder(
        TcpConfiguration configuration,
        Acceptor acceptor,
        Poller poller)
    {
        this.configuration = configuration;
        this.acceptor = acceptor;
        this.poller = poller;
        this.correlations = new Long2ObjectHashMap<>();

        this.framesWrittenByteRouteId = new Long2ObjectHashMap<>();
        this.framesReadByteRouteId = new Long2ObjectHashMap<>();
        this.bytesWrittenByteRouteId = new Long2ObjectHashMap<>();
        this.bytesReadByteRouteId = new Long2ObjectHashMap<>();
    }

    @Override
    public StreamFactoryBuilder setMemoryManager(
        MemoryManager memory)
    {
        this.memory = memory;
        return this;
    }

    @Override
    public StreamFactoryBuilder setDirectBufferBuilderFactory(
        Supplier<DirectBufferBuilder> supplyDirectBufferBuilder)
    {
        this.supplyDirectBufferBuilder = supplyDirectBufferBuilder;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setCorrelationIdSupplier(
        LongSupplier supplyCorrelationId)
    {
        this.supplyCorrelationId = supplyCorrelationId;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setStreamIdSupplier(
        LongSupplier supplyStreamId)
    {
        this.supplyStreamId = supplyStreamId;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public StreamFactoryBuilder setCounterSupplier(
        Function<String, LongSupplier> supplyCounter)
    {
        this.supplyCounter = supplyCounter;
        return this;
    }

    @Override
    public StreamFactoryBuilder setAccumulatorSupplier(
            Function<String, LongConsumer> supplyAccumulator)
    {
        this.supplyAccumulator = supplyAccumulator;
        return this;
    }

    public boolean handleRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        switch(msgTypeId)
        {
            case UnrouteFW.TYPE_ID:
            {
                final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
                final long routeId = unroute.correlationId();
                bytesWrittenByteRouteId.remove(routeId);
                bytesReadByteRouteId.remove(routeId);
                framesWrittenByteRouteId.remove(routeId);
                framesReadByteRouteId.remove(routeId);
            }
            break;
        }
        return true;
    }

    @Override
    public StreamFactory build()
    {
        if (incrementOverflow == null)
        {
            incrementOverflow = supplyCounter.apply("overflows");
        }
        if (supplyWriteFrameCounter == null)
        {
            this.supplyWriteFrameCounter = r ->
            {
                final long routeId = r.correlationId();
                return framesWrittenByteRouteId.computeIfAbsent(
                        routeId,
                        t -> supplyCounter.apply(String.format("%d.frames.written", t)));
            };
            this.supplyReadFrameCounter = r ->
            {
                final long routeId = r.correlationId();
                return framesReadByteRouteId.computeIfAbsent(
                        routeId,
                        t -> supplyCounter.apply(String.format("%d.frames.read", t)));
            };
        }

        if (supplyWriteBytesAccumulator == null)
        {
            this.supplyWriteBytesAccumulator = r ->
            {
                final long routeId = r.correlationId();
                return bytesWrittenByteRouteId.computeIfAbsent(
                        routeId,
                        t -> supplyAccumulator.apply(String.format("%d.bytes.written", t)));
            };
            this.supplyReadBytesAccumulator = r ->
            {
                final long routeId = r.correlationId();
                return bytesReadByteRouteId.computeIfAbsent(
                        routeId,
                        t -> supplyAccumulator.apply(String.format("%d.bytes.read", t)));
            };
        }

        ServerStreamFactory factory = new ServerStreamFactory(
            configuration,
            router,
            poller,
            memory,
            writeBuffer,
            supplyDirectBufferBuilder,
            supplyStreamId,
            supplyCorrelationId,
            correlations,
            incrementOverflow,
            supplyReadFrameCounter,
            supplyReadBytesAccumulator,
            supplyWriteFrameCounter,
            supplyWriteBytesAccumulator);
        acceptor.setServerStreamFactory(factory);
        return factory;

    }


}
