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
package org.reaktivity.nukleus.tcp.internal;

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;

final class TcpNukleus implements Nukleus
{
    static final String NAME = "tcp";

    private final TcpConfiguration config;
    private final Map<RouteKind, MessagePredicate> routeHandlers;
    private final List<TcpElektron> elektrons;
    private final AtomicInteger remainingConnections;

    TcpNukleus(
        TcpConfiguration config)
    {
        this.config = config;
        this.elektrons = new ArrayList<>();
        this.remainingConnections = new AtomicInteger(config.maxConnections());

        Map<RouteKind, MessagePredicate> routeHandlers = new EnumMap<>(RouteKind.class);
        routeHandlers.put(SERVER, this::handleServerRoute);
        routeHandlers.put(CLIENT, this::handleClientRoute);
        this.routeHandlers = routeHandlers;
    }

    @Override
    public String name()
    {
        return TcpNukleus.NAME;
    }

    @Override
    public Configuration config()
    {
        return config;
    }

    @Override
    public MessagePredicate routeHandler(
        RouteKind kind)
    {
        return routeHandlers.get(kind);
    }

    @Override
    public Elektron supplyElektron()
    {
        final TcpElektron elektron = new TcpElektron(config, remainingConnections);
        elektrons.add(elektron);
        return elektron;
    }

    private boolean handleServerRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return elektrons.stream()
                .map(e -> e.routeHandler(SERVER))
                .allMatch(h -> h.test(msgTypeId, buffer, index, length));
    }

    private boolean handleClientRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return elektrons.stream()
                .map(e -> e.routeHandler(CLIENT))
                .allMatch(h -> h.test(msgTypeId, buffer, index, length));
    }
}
