/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.util.Objects.requireNonNull;

import java.nio.channels.SocketChannel;
import java.util.Objects;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.TcpRouteCounters;

public class Correlation
{
    private final SocketChannel channel;
    private final ReadStream readStream;
    private final TcpRouteCounters counters;
    private final Runnable onConnectionClosed;

    private MessageConsumer correlatedStream;
    private long correlatedStreamId;

    public Correlation(
        SocketChannel channel,
        ReadStream readStream,
        MessageConsumer target,
        long targetId,
        TcpRouteCounters counters,
        Runnable onConnectionClosed)
    {
        this.channel = requireNonNull(channel, "channel");
        this.readStream = readStream;
        this.correlatedStream = target;
        this.correlatedStreamId = targetId;
        this.counters = counters;
        this.onConnectionClosed = onConnectionClosed;
    }

    public MessageConsumer correlatedStream()
    {
        return correlatedStream;
    }

    public long correlatedStreamId()
    {
        return correlatedStreamId;
    }

    public SocketChannel channel()
    {
        return channel;
    }

    public Runnable onConnectionClosed()
    {
        return onConnectionClosed;
    }

    public void setCorrelatedThrottle(
        MessageConsumer throttle,
        long streamId)
    {
        readStream.setCorrelatedThrottle(streamId, throttle);
    }

    @Override
    public int hashCode()
    {
        return channel.hashCode();
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return Objects.equals(this.channel, that.channel);
    }

    @Override
    public String toString()
    {
        return String.format("[channel=%s]", channel);
    }

    public TcpRouteCounters counters()
    {
        return counters;
    }
}
