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

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.TcpCounters;
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;

public class TcpClientFactoryBuilder implements StreamFactoryBuilder
{
    private final TcpConfiguration config;
    private final Poller poller;
    private final Long2ObjectHashMap<TcpRouteCounters> countersByRouteId;

    private RouteManager router;
    private Supplier<BufferPool> supplyBufferPool;
    private LongUnaryOperator supplyReplyId;
    private LongSupplier supplyTraceId;
    private ToIntFunction<String> supplyTypeId;

    private MutableDirectBuffer writeBuffer;

    private Function<String, LongSupplier> supplyCounter;
    private Function<String, LongConsumer> supplyAccumulator;

    public TcpClientFactoryBuilder(
        TcpConfiguration config,
        Long2ObjectHashMap<TcpRouteCounters> countersByRouteId,
        Poller poller)
    {
        this.config = config;
        this.countersByRouteId = countersByRouteId;
        this.poller = poller;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public TcpClientFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public TcpClientFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        return this;
    }

    @Override
    public TcpClientFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public TcpClientFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        this.supplyTraceId = supplyTraceId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public TcpClientFactoryBuilder setWriteBuffer(
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

    @Override
    public StreamFactory build()
    {
        final BufferPool bufferPool = supplyBufferPool.get();
        final TcpCounters counters = new TcpCounters(supplyCounter, supplyAccumulator, countersByRouteId);

        return new TcpClientFactory(
            config,
            router,
            poller,
            writeBuffer,
            bufferPool,
            supplyReplyId,
            supplyTraceId,
            supplyTypeId,
            counters);
    }
}
