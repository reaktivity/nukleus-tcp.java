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
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.OUTPUT_NEW;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.LongFunction;
import java.util.function.Predicate;

import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.acceptor.Acceptor;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;
import org.reaktivity.nukleus.tcp.internal.router.RouteKind;

/**
 * The {@code Readable} nukleus reads network traffic via a {@code Source} nukleus and control flow commands
 * from multiple {@code Target} nuklei.
 */
public final class Reader extends Nukleus.Composite
{
    private static final List<Route> EMPTY_ROUTES = emptyList();

    private final Context context;
    private final String sourceName;
    private final Source source;
    private final Map<String, Target> targetsByName;
    private final AtomicBuffer writeBuffer;
    private final Long2ObjectHashMap<List<Route>> routesByRef;


    public Reader(
        Context context,
        Acceptor acceptor,
        Poller poller,
        String sourceName,
        LongFunction<Correlation> resolveCorrelation)
    {
        this.context = context;
        this.sourceName = sourceName;
        this.source = new Source(poller, sourceName, context.maxMessageLength(), resolveCorrelation);
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.targetsByName = new TreeMap<>();
        this.routesByRef = new Long2ObjectHashMap<>();
    }

    @Override
    public String name()
    {
        return String.format("reader[%s]", sourceName);
    }

    public void onAccepted(
        long sourceRef,
        long targetId,
        long correlationId,
        SocketChannel channel,
        SocketAddress address)
    {
        final Predicate<Route> filter =
                sourceMatches(sourceName)
                 .and(addressMatches(address));

        List<Route> routes = routesByRef.get(sourceRef);
        if (routes == null)
        {
            routes = EMPTY_ROUTES;
        }

        final Optional<Route> optional = routes.stream()
            .filter(filter)
            .findFirst();

        if (optional.isPresent())
        {
            final Route route = optional.get();
            final Target target = route.target();
            final long targetRef = route.targetRef();

            source.onBegin(target, targetRef, targetId, correlationId, channel);
        }
        else
        {
            CloseHelper.close(channel);
        }
    }

    public void onConnected(
        long sourceRef,
        String targetName,
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

            source.onBegin(target, targetRef, targetId, correlationId, channel);
        }
        else if (RouteKind.match(sourceRef) == OUTPUT_NEW)
        {
            final Target target = targetsByName.computeIfAbsent(targetName, this::newTarget);

            source.onBegin(target, 0L, targetId, correlationId, channel);
        }
    }

    @Override
    protected void toString(
        StringBuilder builder)
    {
        builder.append(String.format("%s[name=%s]", getClass().getSimpleName(), sourceName));
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

        Target target = new Target(targetName, layout, writeBuffer);
        return include(target);
    }
}
