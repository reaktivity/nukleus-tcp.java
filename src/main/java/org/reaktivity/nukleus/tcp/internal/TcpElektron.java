/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.HashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.stream.Acceptor;
import org.reaktivity.nukleus.tcp.internal.stream.ClientStreamFactoryBuilder;
import org.reaktivity.nukleus.tcp.internal.stream.ServerStreamFactoryBuilder;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;

final class TcpElektron implements Elektron
{
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final Acceptor acceptor;
    private final Poller poller;
    private final Long2ObjectHashMap<TcpRouteCounters> countersByRouteId;
    private final Map<RouteKind, StreamFactoryBuilder> streamFactoryBuilders;
    private final Map<RouteKind, MessagePredicate> routeHandlers;

    TcpElektron(
        TcpConfiguration config)
    {
        final Acceptor acceptor = new Acceptor(config);
        Poller poller = new Poller();
        acceptor.setPoller(poller);

        Long2ObjectHashMap<TcpRouteCounters> countersByRouteId = new Long2ObjectHashMap<>();

        Map<RouteKind, StreamFactoryBuilder> streamFactoryBuilders = new HashMap<>();
        streamFactoryBuilders.put(SERVER, new ServerStreamFactoryBuilder(config, countersByRouteId, acceptor, poller));
        streamFactoryBuilders.put(CLIENT, new ClientStreamFactoryBuilder(config, countersByRouteId, poller));

        Map<RouteKind, MessagePredicate> routeHandlers = new HashMap<>();
        routeHandlers.put(SERVER, this::handleServerRoute);
        routeHandlers.put(CLIENT, this::handleRoute);

        this.acceptor = acceptor;
        this.poller = poller;
        this.streamFactoryBuilders = streamFactoryBuilders;
        this.routeHandlers = routeHandlers;
        this.countersByRouteId = countersByRouteId;
    }

    @Override
    public StreamFactoryBuilder streamFactoryBuilder(
        RouteKind kind)
    {
        return streamFactoryBuilders.get(kind);
    }

    @Override
    public Poller agent()
    {
        return poller;
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), streamFactoryBuilders);
    }

    MessagePredicate routeHandler(
        RouteKind kind)
    {
        return routeHandlers.get(kind);
    }

    private boolean handleServerRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return acceptor.handleRoute(msgTypeId, buffer, index, length) &&
                this.handleRoute(msgTypeId, buffer, index, length);
    }

    private boolean handleRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
            case UnrouteFW.TYPE_ID:
            {
                final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
                final long routeId = unroute.routeId();
                countersByRouteId.remove(routeId);
            }
            break;
        }
        return true;
    }

}