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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.IntSupplier;
import java.util.function.LongFunction;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.nio.TransportPoller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.reader.stream.StreamFactory;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;

@Reaktive
public final class Source extends TransportPoller implements Nukleus
{
    private final String sourceName;
    private final StreamFactory streamFactory;

    public Source(
        String sourceName,
        int bufferSize,
        LongFunction<Correlation> resolveCorrelation)
    {
        this.sourceName = sourceName;
        this.streamFactory = new StreamFactory(bufferSize, resolveCorrelation);
    }

    @Override
    public String name()
    {
        return sourceName;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), sourceName);
    }

    @Override
    public int process()
    {
        int weight = 0;

        try
        {
            selector.selectNow();
            weight += selectedKeySet.forEach(this::processKey);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return weight;
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

            final SelectionKey key = channel.register(selector, 0);
            final IntSupplier attachment = streamFactory.newStream(target, targetId, key, channel, correlationId);

            key.attach(attachment);
        }
        catch (IOException ex)
        {
            CloseHelper.quietClose(channel);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private int processKey(
        SelectionKey selectionKey)
    {
        final IntSupplier attachment = (IntSupplier) selectionKey.attachment();
        return attachment.getAsInt();
    }
}
