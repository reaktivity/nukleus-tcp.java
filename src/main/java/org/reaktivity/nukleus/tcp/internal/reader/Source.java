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
package org.reaktivity.nukleus.tcp.internal.reader;

import static java.nio.channels.SelectionKey.OP_READ;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.function.LongFunction;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.reader.stream.ReaderStreamFactory;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;

public final class Source
{
    private final Poller poller;
    private final String sourceName;
    private final ReaderStreamFactory streamFactory;

    public Source(
        Poller poller,
        String sourceName,
        int maxMessageLength,
        LongFunction<Correlation> resolveCorrelation)
    {
        this.poller = poller;
        this.sourceName = sourceName;
        this.streamFactory = new ReaderStreamFactory(maxMessageLength, resolveCorrelation);
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), sourceName);
    }

    public void onBegin(
        Target target,
        long targetRef,
        long targetId,
        long correlationId,
        SocketChannel channel)
    {
        try
        {
            final InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
            final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();

            target.doTcpBegin(targetId, targetRef, correlationId, localAddress, remoteAddress);

            final PollerKey key = poller.doRegister(channel, 0, null);
            final ToIntFunction<PollerKey> handler = streamFactory.newStream(target, targetId, key, channel, correlationId);

            key.handler(OP_READ, handler);
        }
        catch (IOException ex)
        {
            CloseHelper.quietClose(channel);
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
