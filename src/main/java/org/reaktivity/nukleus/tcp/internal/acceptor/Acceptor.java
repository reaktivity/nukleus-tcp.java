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
import static org.agrona.CloseHelper.quietClose;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.nio.TransportPoller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.router.Router;

/**
 * The {@code Acceptor} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
@Reaktive
public final class Acceptor extends TransportPoller implements Nukleus
{
    private Conductor conductor;
    private Router router;

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

    @Override
    public int process()
    {
        selectNow();
        return selectedKeySet.forEach(this::processAccept);
    }

    @Override
    public String name()
    {
        return "acceptor";
    }

    @Override
    public void close()
    {
        for (SelectionKey key : selector.keys())
        {
            quietClose(key.channel());
        }
        super.close();
    }

    public void doRegister(
        long correlationId,
        String sourceName,
        SocketAddress address)
    {
        try
        {
            final SelectionKey key = findOrRegisterKey(address);

            // TODO: detect collision on different source name for same key
            // TODO: maintain register count
            attach(key, sourceName);

            conductor.onRoutedResponse(correlationId);
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
        final SelectionKey key = findRegisteredKey(address);

        if (Objects.equals(sourceName, attachment(key)))
        {
            // TODO: maintain count for auto close when unregistered for last time
            CloseHelper.quietClose(key.channel());
            selectNowWithoutProcessing();

            conductor.onUnroutedResponse(correlationId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private void selectNow()
    {
        try
        {
            selector.selectNow();
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private int processAccept(
        SelectionKey selectionKey)
    {
        try
        {
            final ServerSocketChannel serverChannel = channel(selectionKey);

            final SocketChannel channel = serverChannel.accept();
            channel.configureBlocking(false);

            final String sourceName = attachment(selectionKey);
            final SocketAddress address = channel.getLocalAddress();

            router.onAccepted(sourceName, channel, address);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return 1;
    }

    private SelectionKey findRegisteredKey(
        SocketAddress localAddress)
    {
        return findSelectionKey(localAddress, a -> null);
    }

    private SelectionKey findOrRegisterKey(
        SocketAddress address)
    {
        return findSelectionKey(address, this::registerKey);
    }

    private SelectionKey findSelectionKey(
        SocketAddress localAddress,
        Function<SocketAddress, SelectionKey> mappingFunction)
    {
        final Optional<SelectionKey> optional =
                selector.keys()
                        .stream()
                        .filter(k -> hasLocalAddress(channel(k), localAddress))
                        .findFirst();

        return optional.orElse(mappingFunction.apply(localAddress));
    }

    private SelectionKey registerKey(
        SocketAddress localAddress)
    {
        try
        {
            final ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(SO_REUSEADDR, true);
            serverChannel.bind(localAddress);
            serverChannel.configureBlocking(false);

            return serverChannel.register(selector, OP_ACCEPT);
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
        SelectionKey selectionKey)
    {
        return (ServerSocketChannel) selectionKey.channel();
    }

    private static String attachment(
        SelectionKey key)
    {
        return key != null ? (String) key.attachment() : null;
    }

    private static void attach(
        SelectionKey key,
        String sourceName)
    {
        key.attach(sourceName);
    }
}
