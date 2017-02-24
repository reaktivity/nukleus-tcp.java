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
package org.reaktivity.nukleus.tcp.internal.writer;

import static java.util.Collections.emptyList;
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.MAXIMUM_STREAMS_WITH_PENDING_WRITES;
import static org.reaktivity.nukleus.tcp.internal.writer.Route.addressMatches;
import static org.reaktivity.nukleus.tcp.internal.writer.Route.sourceMatches;
import static org.reaktivity.nukleus.tcp.internal.writer.Route.sourceRefMatches;
import static org.reaktivity.nukleus.tcp.internal.writer.Route.targetMatches;
import static org.reaktivity.nukleus.tcp.internal.writer.Route.targetRefMatches;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Predicate;

import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.connector.Connector;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;

/**
 * The {@code Writable} nukleus reads streams data from multiple {@code Source} nuklei and monitors completion of
 * partial socket writes via a {@code Target} nukleus.
 */
public final class Writer extends Nukleus.Composite
{
    private static final List<Route> EMPTY_ROUTES = emptyList();

    private final Context context;
    private final Conductor conductor;
    private final Connector connector;
    private final String name;
    private final String sourceName;
    private final AtomicBuffer writeBuffer;
    private final Map<String, Source> sourcesByPartitionName;
    private final Map<String, Target> targetsByName;
    private final Long2ObjectHashMap<List<Route>> routesByRef;
    private final LongFunction<Correlation> resolveCorrelation;

    public Writer(
        Context context,
        Conductor conductor,
        Connector connector,
        String sourceName,
        LongFunction<Correlation> resolveCorrelation)
    {
        this.context = context;
        this.conductor = conductor;
        this.connector = connector;
        this.sourceName = sourceName;
        this.resolveCorrelation = resolveCorrelation;
        this.name = sourceName;
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.sourcesByPartitionName = new HashMap<>();
        this.targetsByName = new HashMap<>();
        this.routesByRef = new Long2ObjectHashMap<>();
    }

    @Override
    public String name()
    {
        return name;
    }

    public void onReadable(
        String partitionName)
    {
        sourcesByPartitionName.computeIfAbsent(partitionName, this::newSource);
    }

    public void onConnected(
        String partitionName,
        long sourceId,
        long sourceRef,
        String targetName,
        long correlationId,
        SocketChannel channel)
    {
        final Source source = sourcesByPartitionName.get(partitionName);
        final Target target = targetsByName.computeIfAbsent(targetName, this::newTarget);

        source.onConnected(sourceId, sourceRef, target, channel, correlationId);
    }

    public void onConnectFailed(
        String partitionName,
        long sourceId)
    {
        Source source = sourcesByPartitionName.get(partitionName);
        source.doReset(sourceId);
    }

    public void doRoute(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        try
        {
            final Target target = targetsByName.computeIfAbsent(targetName, this::newTarget);
            final Route newRoute = new Route(sourceName, sourceRef, target, targetRef, address);

            routesByRef.computeIfAbsent(sourceRef, this::newRoutes)
                       .add(newRoute);

            conductor.onRoutedResponse(correlationId, sourceRef);
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
        InetSocketAddress address)
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
        builder.append(String.format("%s[name=%s]", getClass().getSimpleName(), name));
    }

    private Target newTarget(
        String targetName)
    {
        return include(new Target(targetName));
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

    private Source newSource(
        String partitionName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
            .path(context.captureStreamsPath().apply(partitionName))
            .streamsCapacity(context.streamsBufferCapacity())
            .throttleCapacity(context.throttleBufferCapacity())
            .readonly(true)
            .build();

        Function<String, Target> supplyTarget = n -> targetsByName.computeIfAbsent(n, this::newTarget);

        int maximumPendingWriteStreams = MAXIMUM_STREAMS_WITH_PENDING_WRITES.intValue(() ->
        {
            return (context.maximumStreamsCount() < 1001 ? context.maximumStreamsCount()
                    : context.maximumStreamsCount() / 10);
        });

        return include(new Source(partitionName, connector, this::lookupRoutes, resolveCorrelation,
                       supplyTarget, layout, writeBuffer, maximumPendingWriteStreams,
                       context.counters().overflows()::increment));
    }
}
