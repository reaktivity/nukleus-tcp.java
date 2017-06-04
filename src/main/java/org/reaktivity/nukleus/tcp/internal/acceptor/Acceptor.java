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
package org.reaktivity.nukleus.tcp.internal.acceptor;

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import javax.annotation.Resource;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.router.Router;

/**
 * The {@code Poller} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
@Reaktive
public final class Acceptor implements Nukleus
{
    private final Map<SocketAddress, String> sourcesByLocalAddress;
    private final Function<SocketAddress, PollerKey> registerHandler;
    private final ToIntFunction<PollerKey> acceptHandler;

    private Conductor conductor;
    private Router router;
    private Poller poller;

    public Acceptor()
    {
        this.sourcesByLocalAddress = new HashMap<>();
        this.registerHandler = this::handleRegister;
        this.acceptHandler = this::handleAccept;
    }

    public void setConductor(
        Conductor conductor)
    {
        this.conductor = conductor;
    }

    public void setRouter(
        Router router)
    {
        this.router = router;
    }

    @Resource
    public void setPoller(
        Poller poller)
    {
        this.poller = poller;
    }

    @Override
    public int process()
    {
        return 0;
    }

    @Override
    public String name()
    {
        return "acceptor";
    }

    public void doRegister(
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

            conductor.onRoutedResponse(correlationId, sourceRef);
        }
        catch (Exception ex)
        {
            conductor.onErrorResponse(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnregister(
        long correlationId,
        String sourceName,
        SocketAddress address)
    {
        if (Objects.equals(sourceName, sourcesByLocalAddress.get(address)))
        {
            final PollerKey key = findRegisteredKey(address);

            // TODO: maintain count for auto close when unregistered for last time
            CloseHelper.quietClose(key.channel());

            conductor.onUnroutedResponse(correlationId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private int handleAccept(
        PollerKey key)
    {
        try
        {
            final ServerSocketChannel serverChannel = channel(key);

            final SocketChannel channel = serverChannel.accept();
            channel.configureBlocking(false);

            final InetSocketAddress address = localAddress(channel);
            final String sourceName = sourcesByLocalAddress.get(address);
            final long sourceRef = address.getPort();

            router.onAccepted(sourceName, sourceRef, channel, address);
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
            serverChannel.bind(localAddress);
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
            return channel.getLocalAddress().equals(address);
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
