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

import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.OUTPUT_NEW;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.acceptor.Acceptor;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.connector.Connector;
import org.reaktivity.nukleus.tcp.internal.reader.Reader;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.State;
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
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Map<String, Reader> readers;
    private final Map<String, Writer> writers;
    private final AtomicCounter routesSourced;

    private Conductor conductor;
    private Acceptor acceptor;
    private Connector connector;

    public Router(
        Context context)
    {
        this.context = context;
        this.correlations = new Long2ObjectHashMap<>();
        this.readers = new HashMap<>();
        this.writers = new HashMap<>();
        this.routesSourced = context.counters().routesSourced();
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

    public void doRoute(
        long correlationId,
        Role role,
        State state,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        switch (role)
        {
        case INPUT:
            switch (state)
            {
            case NONE:
            case NEW:
                doRouteInput(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case ESTABLISHED:
                doRouteInputEstablished(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            }
            break;
        case OUTPUT:
            switch (state)
            {
            case NONE:
            case NEW:
                doRouteOutput(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case ESTABLISHED:
                doRouteOutputEstablished(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            }
            break;
        }
    }

    public void doUnroute(
        long correlationId,
        Role role,
        State state,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        switch (role)
        {
        case INPUT:
            switch (state)
            {
            case NONE:
            case NEW:
                doUnrouteInput(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case ESTABLISHED:
                doUnrouteInputEstablished(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            }
            break;
        case OUTPUT:
            switch (state)
            {
            case NONE:
            case NEW:
                doUnrouteOutput(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            case ESTABLISHED:
                doUnrouteOutputEstablished(correlationId, sourceName, sourceRef, targetName, targetRef, address);
                break;
            }
            break;
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
        final Correlation correlation = new Correlation(sourceName, channel);

        correlations.put(correlationId, correlation);

        Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
        reader.onAccepted(targetId, correlationId, channel, address);
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
        // TODO: support network device for channel local address
        targetName = "any";

        Writer writer = writers.get(sourceName);
        writer.onConnected(sourceName, sourceId, sourceRef, targetName, correlationId, channel);

        Reader reader = readers.computeIfAbsent(targetName, this::newReader);
        reader.onConnected(sourceRef, sourceName, targetId, correlationId, channel, address);
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

    private void doRouteInput(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        // TODO: scope address by sourceName device addresses, unless "any" pseudo-device
        if ("any".equals(sourceName) && sourceRef > 0L && sourceRef <= 65535L && address != null)
        {
            Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
            InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
            reader.doRouteAccept(correlationId, sourceRef, targetName, targetRef, localAddress);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doRouteOutput(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        if (targetRef > 0L && targetRef <= 65535L && address == null &&
                (sourceRef == 0 || RouteKind.match(sourceRef) == OUTPUT_NEW))
        {
            if (sourceRef == 0)
            {
                sourceRef = OUTPUT_NEW.nextRef(routesSourced);
            }

            InetSocketAddress remoteAddress = new InetSocketAddress(targetName, (int)targetRef);

            // TODO: support network device for remote address gateway route
            targetName = "any";

            Reader reader = readers.computeIfAbsent(targetName, this::newReader);
            reader.doRouteDefault(correlationId, sourceName);

            Writer writer = writers.computeIfAbsent(sourceName, this::newWriter);
            writer.doRoute(correlationId, sourceRef, targetName, targetRef, remoteAddress);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doRouteOutputEstablished(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        // TODO: scope DNS-resolved targetName by address (mask?)
        if (targetRef == 0L && address == null &&
                (sourceRef == 0 || RouteKind.match(sourceRef) == OUTPUT_ESTABLISHED))
        {
            if (sourceRef == 0)
            {
                sourceRef = OUTPUT_ESTABLISHED.nextRef(routesSourced);
            }

            Writer writer = writers.computeIfAbsent(sourceName, this::newWriter);
            writer.doRoute(correlationId, sourceRef, targetName, targetRef, null);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doRouteInputEstablished(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        if (address == null && sourceRef >= 0L && sourceRef <= 65535L)
        {
            Reader reader = readers.computeIfAbsent(sourceName, this::newReader);
            reader.doRoute(correlationId, sourceRef, targetName, targetRef, null);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteInput(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        Reader reader = readers.get(sourceName);
        if (sourceRef > 0L && sourceRef <= 65535L && address != null && reader != null)
        {
            InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
            reader.doUnrouteAccept(correlationId, sourceRef, targetName, targetRef, localAddress);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteOutput(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        Writer writer = writers.get(sourceName);
        if (writer != null && address == null)
        {
            InetSocketAddress remoteAddress = new InetSocketAddress(targetName, (int)targetRef);

            // TODO: support network device for remote address gateway route
            targetName = "any";

            writer.doUnroute(correlationId, sourceRef, targetName, targetRef, remoteAddress);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteOutputEstablished(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        Writer writer = writers.get(sourceName);
        if (targetRef == 0L && address == null && writer != null)
        {
            writer.doUnroute(correlationId, sourceRef, targetName, targetRef, null);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void doUnrouteInputEstablished(
        long correlationId,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        InetAddress address)
    {
        Reader reader = readers.get(sourceName);
        if (address == null && sourceRef >= 0L && sourceRef <= 65535L && reader != null)
        {
            reader.doUnroute(correlationId, sourceRef, targetName, targetRef, null);
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
        return include(new Writer(context, conductor, connector, sourceName, correlations::remove));
    }
}
