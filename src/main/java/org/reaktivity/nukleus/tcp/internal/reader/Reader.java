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

import static java.util.Collections.emptyList;
import static org.reaktivity.nukleus.tcp.internal.reader.Route.addressMatches;
import static org.reaktivity.nukleus.tcp.internal.reader.Route.sourceMatches;
import static org.reaktivity.nukleus.tcp.internal.reader.Route.sourceRefMatches;
import static org.reaktivity.nukleus.tcp.internal.reader.Route.targetMatches;
import static org.reaktivity.nukleus.tcp.internal.reader.Route.targetRefMatches;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.acceptor.Acceptor;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;

/**
 * The {@code Readable} nukleus reads network traffic via a {@code Source} nukleus and control flow commands
 * from multiple {@code Target} nuklei.
 */
@Reaktive
public final class Reader extends Nukleus.Composite
{
    private static final List<Route> EMPTY_ROUTES = emptyList();

    private final Context context;
    private final Conductor conductor;
    private final Acceptor acceptor;
    private final String sourceName;
    private final Source source;
    private final Map<String, Target> targetsByName;
    private final AtomicBuffer writeBuffer;
    private final Long2ObjectHashMap<List<Route>> routesByRef;

    public Reader(
        Context context,
        Conductor conductor,
        Acceptor acceptor,
        String sourceName)
    {
        this.context = context;
        this.conductor = conductor;
        this.acceptor = acceptor;
        this.sourceName = sourceName;
        this.source = include(new Source(sourceName, context.maxMessageLength()));
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.targetsByName = new TreeMap<>();
        this.routesByRef = new Long2ObjectHashMap<>();
    }

    @Override
    public String name()
    {
        return String.format("reader[%s]", sourceName);
    }

    public void onConnected(
        long targetId,
        long correlationId,
        SocketChannel channel,
        SocketAddress address)
    {
        final Predicate<Route> filter =
                sourceMatches(sourceName)
                 .and(addressMatches(address));

        final Optional<Route> optional = routesByRef.values().stream()
            .flatMap(rs -> rs.stream())
            .filter(filter)
            .findFirst();

        if (optional.isPresent())
        {
            final Route route = optional.get();
            final Target target = route.target();
            final long targetRef = route.targetRef();

            source.doBegin(target, targetRef, targetId, correlationId, channel);
        }
    }

    public void doRouteAccept(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        SocketAddress address)
    {
        try
        {
            final Target target = targetsByName.computeIfAbsent(targetName, this::newTarget);
            final Route newRoute = new Route(sourceName, sourceRef, target, targetRef, address);

            routesByRef.computeIfAbsent(sourceRef, this::newRoutes)
                       .add(newRoute);

            acceptor.doRegister(correlationId, sourceName, address);
        }
        catch (Exception ex)
        {
            conductor.onErrorResponse(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnrouteAccept(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        SocketAddress address)
    {
        final List<Route> routes = lookupRoutes(sourceRef);

        final Predicate<Route> filter =
                sourceMatches(sourceName)
                 .and(sourceRefMatches(sourceRef))
                 .and(targetMatches(targetName))
                 .and(targetRefMatches(targetRef))
                 .and(addressMatches(address));

        if (routes.removeIf(filter))
        {
            acceptor.doUnregister(correlationId, sourceName, address);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void doRoute(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        SocketAddress address)
    {
        try
        {
            final Target target = targetsByName.computeIfAbsent(targetName, this::newTarget);
            final Route newRoute = new Route(sourceName, sourceRef, target, targetRef, address);

            routesByRef.computeIfAbsent(sourceRef, this::newRoutes)
                       .add(newRoute);

            conductor.onRoutedResponse(correlationId);
        }
        catch (Exception ex)
        {
            conductor.onErrorResponse(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnroute(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        SocketAddress address)
    {
        final List<Route> routes = lookupRoutes(sourceRef);

        final Predicate<Route> filter =
                sourceMatches(sourceName)
                 .and(sourceRefMatches(sourceRef))
                 .and(targetMatches(targetName))
                 .and(targetRefMatches(targetRef))
                 .and(addressMatches(address));

        if (routes.removeIf(filter))
        {
            conductor.onUnroutedResponse(correlationId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    @Override
    protected void toString(
        StringBuilder builder)
    {
        builder.append(String.format("%s[name=%s]", getClass().getSimpleName(), sourceName));
    }

    private List<Route> newRoutes(
        long sourceRef)
    {
        return new ArrayList<>();
    }

    private List<Route> lookupRoutes(
        long referenceId)
    {
        return routesByRef.getOrDefault(referenceId, EMPTY_ROUTES);
    }

    private Target newTarget(
        String targetName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(context.routeStreamsPath().apply(sourceName, targetName))
                .streamsCapacity(context.streamsBufferCapacity())
                .throttleCapacity(context.throttleBufferCapacity())
                .readonly(false)
                .build();

        return include(new Target(targetName, layout, writeBuffer));
    }
}
