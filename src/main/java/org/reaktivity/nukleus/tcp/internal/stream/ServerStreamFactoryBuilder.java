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

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

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

public class ServerStreamFactoryBuilder implements StreamFactoryBuilder
{
    private final Acceptor acceptor;
    private final TcpConfiguration config;
    private final Poller poller;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Long2ObjectHashMap<TcpRouteCounters> countersByRouteId;

    private RouteManager router;
    private LongUnaryOperator supplyInitialId;
    private LongUnaryOperator supplyReplyId;
    private LongSupplier supplyTrace;
    private Supplier<BufferPool> supplyBufferPool;
    private MutableDirectBuffer writeBuffer;
    private LongFunction<IntUnaryOperator> groupBudgetClaimer;
    private LongFunction<IntUnaryOperator> groupBudgetReleaser;
    private Function<String, LongSupplier> supplyCounter;
    private Function<String, LongConsumer> supplyAccumulator;

    public ServerStreamFactoryBuilder(
        TcpConfiguration config,
        Long2ObjectHashMap<TcpRouteCounters> countersByRouteId,
        Acceptor acceptor,
        Poller poller)
    {
        this.config = config;
        this.countersByRouteId = countersByRouteId;
        this.acceptor = acceptor;
        this.poller = poller;
        this.correlations = new Long2ObjectHashMap<>();
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
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
    public ServerStreamFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        this.supplyInitialId = supplyInitialId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setTraceSupplier(
        LongSupplier supplyTrace)
    {
        this.supplyTrace = supplyTrace;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setGroupBudgetClaimer(
        LongFunction<IntUnaryOperator> groupBudgetClaimer)
    {
        this.groupBudgetClaimer = groupBudgetClaimer;
        return this;
    }

    @Override
    public ServerStreamFactoryBuilder setGroupBudgetReleaser(
        LongFunction<IntUnaryOperator> groupBudgetReleaser)
    {
        this.groupBudgetReleaser = groupBudgetReleaser;
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

    @Override
    public StreamFactory build()
    {
        final BufferPool bufferPool = supplyBufferPool.get();
        final TcpCounters counters = new TcpCounters(supplyCounter, supplyAccumulator, countersByRouteId);

        ServerStreamFactory factory = new ServerStreamFactory(
            config,
            router,
            writeBuffer,
            bufferPool,
            supplyInitialId,
            supplyTrace,
            supplyReplyId,
            correlations,
            poller,
            groupBudgetClaimer,
            groupBudgetReleaser,
            counters);
        acceptor.setServerStreamFactory(factory);
        acceptor.setRouter(router);
        return factory;

    }


}
