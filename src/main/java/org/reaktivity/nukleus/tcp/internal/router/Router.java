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
package org.reaktivity.nukleus.tcp.internal.router;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.acceptor.Acceptor;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.connector.Connector;
import org.reaktivity.nukleus.tcp.internal.reader.Reader;
import org.reaktivity.nukleus.tcp.internal.writer.Writer;

/**
 * The {@code Router} nukleus manages in-bound and out-bound routes, coordinating with the {@code Acceptable},
 * {@code Readable} and {@code Writable} nuklei as needed.
 */
@Reaktive
public final class Router extends Nukleus.Composite
{
    private static final Pattern SOURCE_NAME = Pattern.compile("([^#]+).*");

    private final Context context;
    private final LongHashSet referenceIds;
    private final Long2ObjectHashMap<SocketChannel> correlationIds;
    private final Map<String, Reader> readers;
    private final Map<String, Writer> writers;

    private Conductor conductor;
    private Acceptor acceptor;
    private Connector connector;

    public Router(
        Context context)
    {
        this.context = context;
        this.referenceIds = new LongHashSet(-1L);
        this.correlationIds = new Long2ObjectHashMap<>();
        this.readers = new HashMap<>();
        this.writers = new HashMap<>();
    }

    public void setConductor(Conductor conductor)
    {
        this.conductor = conductor;
    }

    public void setAcceptor(Acceptor acceptor)
    {
        this.acceptor = acceptor;
    }

    public void setConnector(Connector connector)
    {
        this.connector = connector;
    }

    @Override
    public String name()
    {
        return "router";
    }

    @Override
    protected void toString(
        StringBuilder builder)
    {
        builder.append(getClass().getSimpleName());
    }

    public void doBind(
        long correlationId,
        int kind)
    {
        final AtomicCounter targetsBound = context.counters().targetsBound();

        final RouteKind routeKind = RouteKind.of(kind);
        final long referenceId = routeKind.nextRef(targetsBound);

        if (referenceIds.add(referenceId))
        {
            conductor.onBoundResponse(correlationId, referenceId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void doUnbind(
        long correlationId,
        long referenceId)
    {
        if (referenceIds.remove(referenceId))
        {
            conductor.onUnboundResponse(correlationId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void doRoute(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        if (referenceIds.contains(sourceRef))
        {
            switch (RouteKind.match(sourceRef))
            {
            case SERVER_INITIAL:
                doRouteAcceptor(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case SERVER_REPLY:
            case CLIENT_INITIAL:
                doRouteWriter(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case CLIENT_REPLY:
                doRouteReader(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            default:
                conductor.onErrorResponse(correlationId);
                break;
            }
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void doUnroute(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        if (referenceIds.contains(sourceRef))
        {
            switch (RouteKind.match(sourceRef))
            {
            case SERVER_INITIAL:
                doUnrouteAcceptor(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case SERVER_REPLY:
            case CLIENT_INITIAL:
                doUnrouteWriter(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case CLIENT_REPLY:
                doUnrouteReader(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            default:
                conductor.onErrorResponse(correlationId);
                break;
            }
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void onAccepted(
        String sourceName,
        SocketChannel channel,
        SocketAddress address)
    {
        final AtomicCounter streamsSourced = context.counters().streamsSourced();
        final long targetId = streamsSourced.increment();
        final long correlationId = System.identityHashCode(channel);

        correlationIds.put(correlationId, channel);

        Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
        reader.onConnected(targetId, correlationId, channel, address);
    }

    public void onConnected(
        String sourceName,
        long sourceRef,
        long sourceId,
        String targetName,
        long targetId,
        long targetRef,
        long correlationId,
        SocketChannel channel,
        InetSocketAddress address)
    {
        Writer writer = writers.get(sourceName);
        writer.onConnected(sourceName, sourceId, sourceRef, targetName, correlationId, channel);

        Reader reader = readers.computeIfAbsent(targetName, this::newReader);
        reader.onConnected(targetId, correlationId, channel, address);
    }

    public void onConnectFailed(
        String sourceName,
        long sourceId)
    {
        Writer writer = writers.get(sourceName);
        writer.onConnectFailed(sourceName, sourceId);
    }

    public void onReadable(
        Path sourcePath)
    {
        String sourceName = source(sourcePath);
        Writer writer = writers.computeIfAbsent(sourceName, this::newWriter);
        String partitionName = sourcePath.getFileName().toString();
        writer.onReadable(partitionName);
    }

    public void onExpired(
        Path sourcePath)
    {
        // TODO:
    }

    private static String source(
        Path path)
    {
        Matcher matcher = SOURCE_NAME.matcher(path.getName(path.getNameCount() - 1).toString());
        if (matcher.matches())
        {
            return matcher.group(1);
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private void doRouteAcceptor(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
        reader.doRouteAccept(correlationId, sourceRef, targetName, targetRef, address);
    }

    private void doRouteWriter(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Writer writer = writers.computeIfAbsent(sourceName, this::newWriter);
        writer.doRoute(correlationId, sourceRef, targetName, targetRef, address);
    }

    private void doRouteReader(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
        reader.doRoute(correlationId, sourceRef, targetName, targetRef, address);
    }

    private void doUnrouteAcceptor(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Reader reader = readers.get(sourceName);
        if (reader != null)
        {
            reader.doUnrouteAccept(correlationId, sourceRef, targetName, targetRef, address);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteWriter(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Writer writer = writers.get(sourceName);
        if (writer != null)
        {
            writer.doUnroute(correlationId, sourceRef, targetName, targetRef, address);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteReader(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetSocketAddress address)
    {
        Reader reader = readers.get(sourceName);
        if (reader != null)
        {
            reader.doUnroute(correlationId, sourceRef, targetName, targetRef, address);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private Reader newReader(
        String sourceName)
    {
        return include(new Reader(context, conductor, acceptor, sourceName));
    }

    private Writer newWriter(
        String sourceName)
    {
        return include(new Writer(context, conductor, connector, sourceName, correlationIds::remove));
    }
}
