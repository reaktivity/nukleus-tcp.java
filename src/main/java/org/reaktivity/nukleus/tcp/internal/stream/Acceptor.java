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

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.compareAddresses;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.inetAddress;

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
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.TcpRouteExFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.tcp.internal.util.IpUtil;

/**
 * The {@code Poller} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
public final class Acceptor
{
    private final RouteFW routeRO = new RouteFW();
    private final TcpRouteExFW routeExRO = new TcpRouteExFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final int backlog;
    private final Map<SocketAddress, String> sourcesByLocalAddress;
    private final Function<SocketAddress, PollerKey> registerHandler;
    private final ToIntFunction<PollerKey> acceptHandler;

    private Poller poller;
    private ServerStreamFactory serverStreamFactory;

    public Acceptor(
        TcpConfiguration config)
    {
        this.backlog = config.maximumBacklog();
        this.sourcesByLocalAddress = new TreeMap<>(IpUtil::compareAddresses);
        this.registerHandler = this::handleRegister;
        this.acceptHandler = this::handleAccept;
    }

    public void setPoller(
        Poller poller)
    {
        this.poller = poller;
    }

    public boolean handleRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
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
                final OctetsFW extension = route.extension();

                final TcpRouteExFW routeEx = extension.get(routeExRO::wrap);
                final InetAddress address = inetAddress(routeEx.address());
                InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
                result = doRegister(correlationId, source, sourceRef, localAddress);
            }
            break;
        case UnrouteFW.TYPE_ID:
            {
                final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
                assert unroute.role().get() == Role.SERVER;
                final long correlationId = unroute.correlationId();
                final String source = unroute.source().asString();
                final long sourceRef = unroute.sourceRef();
                final OctetsFW extension = unroute.extension();

                final TcpRouteExFW unrouteEx = extension.get(routeExRO::wrap);
                final InetAddress address = inetAddress(unrouteEx.address());
                if (sourceRef > 0L && sourceRef <= 65535L)
                {
                    InetSocketAddress localAddress = new InetSocketAddress(address, (int)sourceRef);
                    result = doUnregister(correlationId, source, localAddress);
                }
                else
                {
                    result = false;
                }
            }
            break;

        }
        return result;
    }

    void setServerStreamFactory(ServerStreamFactory serverStreamFactory)
    {
        this.serverStreamFactory = serverStreamFactory;

    }

    private boolean doRegister(
        long correlationId,
        String sourceName,
        long sourceRef,
        SocketAddress address)
    {
        try
        {
            findOrRegisterKey(address);

            // TODO: detect collision on different source name for same key
            // TODO: maintain register count
            sourcesByLocalAddress.putIfAbsent(address, sourceName);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        return true;
    }

    private boolean doUnregister(
        long correlationId,
        String sourceName,
        SocketAddress address)
    {
        boolean result;
        if (Objects.equals(sourceName, sourcesByLocalAddress.get(address)))
        {
            final PollerKey key = findRegisteredKey(address);

            // TODO: maintain count for auto close when unregistered for last time
            CloseHelper.quietClose(key.channel());
            result = true;

        }
        else
        {
            result = false;
        }
        return result;
    }

    private int handleAccept(
        PollerKey key)
    {
        try
        {
            final ServerSocketChannel serverChannel = channel(key);

            final SocketChannel channel = serverChannel.accept();
            channel.configureBlocking(false);
            channel.setOption(TCP_NODELAY, true);

            final InetSocketAddress address = localAddress(channel);
            final String sourceName = sourcesByLocalAddress.get(address);
            final long sourceRef = address.getPort();

            serverStreamFactory.onAccepted(sourceName, sourceRef, channel, address);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return 1;
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
