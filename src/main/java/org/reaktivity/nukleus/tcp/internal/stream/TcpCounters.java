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
package org.reaktivity.nukleus.tcp.internal.stream;

import java.util.function.LongConsumer;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.reaktor.nukleus.ElektronContext;

public final class TcpCounters
{
    private final ElektronContext context;
    private final Long2ObjectHashMap<TcpRouteCounters> countersByRouteId;

    public final LongConsumer connections;

    public TcpCounters(
        ElektronContext context)
    {
        this.context = context;
        this.countersByRouteId = new Long2ObjectHashMap<>();

        this.connections = context.supplyAccumulator("tcp.connections");
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
        return new TcpRouteCounters(context, routeId);
    }
}
