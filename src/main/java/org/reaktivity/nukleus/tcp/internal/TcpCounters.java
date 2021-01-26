/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.collections.Long2ObjectHashMap;

public final class TcpCounters
{
    private final Function<String, LongSupplier> supplyCounter;
    private final Function<String, LongConsumer> supplyAccumulator;
    private final Long2ObjectHashMap<TcpRouteCounters> countersByRouteId;

    public final LongConsumer connections;

    public TcpCounters(
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator,
        Long2ObjectHashMap<TcpRouteCounters> countersByRouteId)
    {
        this.supplyCounter = supplyCounter;
        this.supplyAccumulator = supplyAccumulator;
        this.countersByRouteId = countersByRouteId;

        this.connections = supplyAccumulator.apply("tcp.connections");
    }

    public TcpRouteCounters supplyRoute(
        long routeId)
    {
        return countersByRouteId.computeIfAbsent(routeId, this::newRouteCounters);
    }

    public TcpRouteCounters removeRoute(
        long routeId)
    {
        return countersByRouteId.remove(routeId);
    }

    private TcpRouteCounters newRouteCounters(
        long routeId)
    {
        return new TcpRouteCounters(routeId, supplyCounter, supplyAccumulator);
    }
}
