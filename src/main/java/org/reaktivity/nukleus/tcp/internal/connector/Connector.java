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
package org.reaktivity.nukleus.tcp.internal.connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.agrona.LangUtil;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.nio.TransportPoller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.router.Router;

/**
 * The {@code Connector} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
@Reaktive
public final class Connector extends TransportPoller implements Nukleus
{
    private final Context context;

    private Router router;

    public Connector(
        Context context)
    {
        this.context = context;
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
        return selectedKeySet.forEach(this::processConnect);
    }

    @Override
    public String name()
    {
        return "connector";
    }

    public void doConnect(
        String sourceName,
        long sourceRef,
        long sourceId,
        long correlationId,
        String targetName,
        long targetRef,
        SocketChannel channel,
        InetSocketAddress remoteAddress)
    {
        final Request request =
                new Request(sourceName, sourceRef, sourceId, targetName, targetRef, correlationId, channel, remoteAddress);

        try
        {
            if (channel.connect(remoteAddress))
            {
                handleConnected(request);
            }
            else
            {
                channel.register(selector, SelectionKey.OP_CONNECT, request);
            }
        }
        catch (IOException ex)
        {
            handleConnectFailed(request);
            LangUtil.rethrowUnchecked(ex);
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

    private int processConnect(
        SelectionKey selectionKey)
    {
        final Request request = (Request) selectionKey.attachment();
        final SocketChannel channel = request.channel();

        try
        {
            channel.finishConnect();
            handleConnected(request);
        }
        catch (Exception ex)
        {
            handleConnectFailed(request);
            LangUtil.rethrowUnchecked(ex);
        }

        return 1;
    }

    private void handleConnected(
        Request request)
    {
        final AtomicCounter streamsSourced = context.counters().streams();
        final String sourceName = request.sourceName();
        final long sourceRef = request.sourceRef();
        final long sourceId = request.sourceId();
        final String targetName = request.targetName();
        final long targetRef = request.targetRef();
        final long targetId = streamsSourced.increment();
        final long correlationId = request.correlationId();
        final SocketChannel channel = request.channel();
        final InetSocketAddress address = request.address();

        router.onConnected(sourceName, sourceRef, sourceId, targetName, targetId, targetRef, correlationId, channel, address);
    }

    private void handleConnectFailed(
        Request request)
    {
        final String sourceName = request.sourceName();
        final long sourceId = request.sourceId();

        router.onConnectFailed(sourceName, sourceId);
    }
}
