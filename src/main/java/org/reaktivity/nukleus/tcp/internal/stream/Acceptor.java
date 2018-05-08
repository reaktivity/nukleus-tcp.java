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
package org.reaktivity.nukleus.tcp.internal.stream;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.net.StandardSocketOptions.SO_REUSEPORT;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.tcp.internal.util.IpUtil;

/**
 * The {@code Poller} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
public final class Acceptor
{
    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final int backlog;
    private final int maxConnections;
    private final boolean tcpKeepalive;
    private final Map<SocketAddress, String> sourcesByLocalAddress;
    private final Function<SocketAddress, PollerKey> registerHandler;
    private final ToIntFunction<PollerKey> acceptHandler;
    private int connections;

    private Poller poller;
    private ServerStreamFactory serverStreamFactory;
    private RouteManager router;
    private boolean unbound;

    public Acceptor(
        TcpConfiguration config)
    {
        this.backlog = config.maximumBacklog();
        this.maxConnections = config.maxConnections();
        this.tcpKeepalive = config.tcpKeepalive();
        this.sourcesByLocalAddress = new TreeMap<>(IpUtil::compareAddresses);
        this.registerHandler = this::handleRegister;
        this.acceptHandler = this::handleAccept;
    }

    public void setPoller(
        Poller poller)
    {
        this.poller = poller;
    }

    public boolean handleRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean result = true;
        switch(msgTypeId)
        {
        case RouteFW.TYPE_ID:
            {
                final RouteFW route = routeRO.wrap(buffer, index, index + length);
                assert route.role().get() == Role.SERVER;
                final long correlationId = route.correlationId();
                final String source = route.source().asString();
                final long sourceRef = route.sourceRef();
                result = sourceRef <= 0 ? false : doRegister(correlationId, source, sourceRef);
            }
            break;
        case UnrouteFW.TYPE_ID:
            {
                final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
                assert unroute.role().get() == Role.SERVER;
                final String source = unroute.source().asString();
                final long sourceRef = unroute.sourceRef();
                result = doUnregister(source, sourceRef);
            }
            break;

        }
        return result;
    }

    void setServerStreamFactory(
        ServerStreamFactory serverStreamFactory)
    {
        this.serverStreamFactory = serverStreamFactory;

    }

    void setRouter(
        RouteManager router)
    {
        this.router = router;
    }

    private boolean doRegister(
        long correlationId,
        String sourceName,
        long sourceRef)
    {
        try
        {
            final InetAddress address = InetAddress.getByName(sourceName);
            final InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
            findOrRegisterKey(localAddress);

            // TODO: detect collision on different source name for same key
            // TODO: maintain register count
            sourcesByLocalAddress.putIfAbsent(localAddress, sourceName);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        return true;
    }

    private boolean doUnregister(
        String sourceName,
        long sourceRef)
    {
        boolean result = false;
        try
        {
            InetAddress address = InetAddress.getByName(sourceName);
            final InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
            if (Objects.equals(sourceName, sourcesByLocalAddress.get(localAddress)))
            {
                final PollerKey key = findRegisteredKey(localAddress);

                // TODO: maintain count for auto close when unregistered for last time
                CloseHelper.quietClose(key.channel());
                result = true;
            }
        }
        catch (final Exception ignore)
        {
            // NOOP
        }
        return result;
    }

    private int handleAccept(
        PollerKey key)
    {
        try
        {
            final ServerSocketChannel serverChannel = channel(key);

            for (SocketChannel channel = accept(serverChannel); channel != null; channel = accept(serverChannel))
            {
                channel.configureBlocking(false);
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, tcpKeepalive);

                final InetSocketAddress address = localAddress(channel);
                final String sourceName = sourcesByLocalAddress.get(address);
                final long sourceRef = address.getPort();

                serverStreamFactory.onAccepted(sourceName, sourceRef, channel, address, this::connectionDone);
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return 1;
    }

    // @return null if max connections are reached or no more accept channels
    private SocketChannel accept(
        ServerSocketChannel serverChannel) throws Exception
    {
        SocketChannel channel = null;

        if (!unbound && connections >= maxConnections)
        {
            router.forEach((id, buffer, index, length) ->
            {
                RouteFW route = routeRO.wrap(buffer, index, index + length);
                if (route.role().get() == Role.SERVER)
                {
                    doUnregister(routeRO.source().asString(), routeRO.sourceRef());
                }
            });
            unbound = true;
        }
        else
        {
            channel = serverChannel.accept();
            if (channel != null)
            {
                connections++;
                serverStreamFactory.connectionsAccumulator.accept(1);
            }
        }

        return channel;
    }

    private void connectionDone()
    {
        connections--;
        assert connections >= 0;
        serverStreamFactory.connectionsAccumulator.accept(-1);
        if (unbound && connections < maxConnections)
        {
            router.forEach((id, buffer, index, length) ->
            {
                RouteFW route = routeRO.wrap(buffer, index, index + length);
                if (route.role().get() == Role.SERVER)
                {
                    doRegister(routeRO.correlationId(), routeRO.source().asString(), routeRO.sourceRef());
                }
            });
            unbound = false;
        }
    }

    private PollerKey findRegisteredKey(
        SocketAddress localAddress)
    {
        return findPollerKey(localAddress, a -> null);
    }

    private PollerKey findOrRegisterKey(
        SocketAddress address)
    {
        return findPollerKey(address, registerHandler);
    }

    private PollerKey findPollerKey(
        SocketAddress localAddress,
        Function<SocketAddress, PollerKey> mappingFunction)
    {
        final Optional<PollerKey> optional =
                poller.keys()
                      .filter(PollerKey::isValid)
                      .filter(k -> ServerSocketChannel.class.isInstance(k.channel()))
                      .filter(k -> hasLocalAddress(channel(k), localAddress))
                      .findFirst();

        return optional.orElse(mappingFunction.apply(localAddress));
    }

    private PollerKey handleRegister(
        SocketAddress localAddress)
    {
        try
        {
            final ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(SO_REUSEADDR, true);
            serverChannel.setOption(SO_REUSEPORT, true);
            serverChannel.bind(localAddress, backlog);
            serverChannel.configureBlocking(false);

            return poller.doRegister(serverChannel, OP_ACCEPT, acceptHandler);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }

    private boolean hasLocalAddress(
        NetworkChannel channel,
        SocketAddress address)
    {
        try
        {
            return compareAddresses(channel.getLocalAddress(), address) == 0;
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return false;
    }

    private static ServerSocketChannel channel(
        PollerKey key)
    {
        return (ServerSocketChannel) key.channel();
    }

    private static InetSocketAddress localAddress(
        SocketChannel channel) throws IOException
    {
        return (InetSocketAddress) channel.getLocalAddress();
    }

}
