/**
 * Copyright 2016-2021 The Reaktivity Project
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
import static java.net.StandardSocketOptions.SO_REUSEPORT;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static org.reaktivity.reaktor.config.Role.SERVER;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.tcp.internal.TcpConfiguration;
import org.reaktivity.nukleus.tcp.internal.config.TcpBinding;
import org.reaktivity.nukleus.tcp.internal.config.TcpOptions;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.poller.PollerKey;

public final class TcpServerRouter
{
    private final TcpCounters counters;
    private final Long2ObjectHashMap<TcpBinding> bindings;
    private final ToIntFunction<PollerKey> acceptHandler;
    private final Function<SelectableChannel, PollerKey> supplyPollerKey;

    private int remainingConnections;
    private boolean unbound;

    public TcpServerRouter(
        TcpConfiguration config,
        ElektronContext context,
        ToIntFunction<PollerKey> acceptHandler)
    {
        this.remainingConnections = config.maxConnections();
        this.counters = new TcpCounters(context);
        this.bindings = new Long2ObjectHashMap<>();
        this.supplyPollerKey = context::supplyPollerKey;
        this.acceptHandler = acceptHandler;
    }

    public void attach(
        TcpBinding binding)
    {
        bindings.put(binding.routeId, binding);

        register(binding);
    }

    public TcpBinding resolve(
        long routeId,
        long authorization)
    {
        return bindings.get(routeId);
    }

    public void detach(
        long routeId)
    {
        TcpBinding binding = bindings.remove(routeId);
        unregister(binding);
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), bindings);
    }

    public SocketChannel accept(
        ServerSocketChannel server) throws IOException
    {
        SocketChannel channel = null;

        if (remainingConnections > 0)
        {
            channel = server.accept();

            if (channel != null)
            {
                remainingConnections--;
                counters.connections.accept(1);
            }
        }

        if (!unbound && remainingConnections <= 0)
        {
            bindings.values().stream()
                .filter(b -> b.kind == SERVER)
                .forEach(this::unregister);
            unbound = true;
        }

        return channel;
    }

    public void close(
        SocketChannel channel)
    {
        CloseHelper.quietClose(channel);
        remainingConnections++;
        counters.connections.accept(-1);

        if (unbound && remainingConnections > 0)
        {
            bindings.values().stream()
                .filter(b -> b.kind == SERVER)
                .forEach(this::register);
            unbound = false;
        }
    }

    private void register(
        TcpBinding tcpBinding)
    {
        try
        {
            TcpOptions options = tcpBinding.options;
            InetAddress address = InetAddress.getByName(options.host);
            InetSocketAddress local = new InetSocketAddress(address, options.port);

            ServerSocketChannel channel = ServerSocketChannel.open();
            channel.setOption(SO_REUSEADDR, true);
            channel.setOption(SO_REUSEPORT, true);
            channel.bind(local, options.backlog);
            channel.configureBlocking(false);

            PollerKey acceptKey = supplyPollerKey.apply(channel);
            acceptKey.handler(OP_ACCEPT, acceptHandler);
            acceptKey.register(OP_ACCEPT);

            tcpBinding.attach(acceptKey);
            acceptKey.attach(tcpBinding);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void unregister(
        TcpBinding binding)
    {
        PollerKey acceptKey = binding.attach(null);
        CloseHelper.quietClose(acceptKey.channel());
    }
}
