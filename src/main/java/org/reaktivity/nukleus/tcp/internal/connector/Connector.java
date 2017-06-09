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

import static java.nio.channels.SelectionKey.OP_CONNECT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.router.Router;

/**
 * The {@code Connector} nukleus accepts new socket connections and informs the {@code Router} nukleus.
 */
public final class Connector implements Nukleus
{
    private final LongSupplier supplyTargetId;

    private Router router;
    private Poller poller;

    public Connector(
        Context context)
    {
        this.supplyTargetId = context.counters().streams()::increment;
    }

    public void setRouter(
        Router router)
    {
        this.router = router;
    }

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
                poller.doRegister(channel, OP_CONNECT, request);
            }
        }
        catch (IOException ex)
        {
            handleConnectFailed(request);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void handleConnected(
        Request request)
    {
        final String sourceName = request.sourceName();
        final long sourceRef = request.sourceRef();
        final long sourceId = request.sourceId();
        final String targetName = request.targetName();
        final long targetRef = request.targetRef();
        final long targetId = supplyTargetId.getAsLong();
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

    private final class Request implements ToIntFunction<PollerKey>
    {
        private final String sourceName;
        private final long sourceRef;
        private final long sourceId;
        private final String targetName;
        private final long targetRef;
        private final long correlationId;
        private final SocketChannel channel;
        private final InetSocketAddress address;

        private Request(
            String sourceName,
            long sourceRef,
            long sourceId,
            String targetName,
            long targetRef,
            long correlationId,
            SocketChannel channel,
            InetSocketAddress address)
        {
            this.sourceName = sourceName;
            this.sourceRef = sourceRef;
            this.sourceId = sourceId;
            this.targetName = targetName;
            this.targetRef = targetRef;
            this.correlationId = correlationId;
            this.channel = channel;
            this.address = address;
        }

        public String sourceName()
        {
            return sourceName;
        }

        public long sourceRef()
        {
            return sourceRef;
        }

        public long sourceId()
        {
            return sourceId;
        }

        public String targetName()
        {
            return targetName;
        }

        public long targetRef()
        {
            return targetRef;
        }

        public long correlationId()
        {
            return correlationId;
        }

        public SocketChannel channel()
        {
            return channel;
        }

        public InetSocketAddress address()
        {
            return address;
        }

        @Override
        public String toString()
        {
            return String.format(
                    "[sourceName=%s, sourceRef=%d, sourceId=%d, targetName=%s, targetRef=%d" +
                            ", correlationId=%d, channel=%s, address=%s]",
                    sourceName, sourceRef, sourceId, targetName, targetRef, correlationId, channel, address);
        }

        @Override
        public int applyAsInt(
            PollerKey key)
        {
            try
            {
                channel.finishConnect();
                handleConnected(this);
            }
            catch (Exception ex)
            {
                handleConnectFailed(this);
            }
            finally
            {
                key.cancel(OP_CONNECT);
            }

            return 1;
        }
    }
}
