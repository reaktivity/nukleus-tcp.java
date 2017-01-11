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

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class Request
{
    private final String sourceName;
    private final long sourceRef;
    private final long sourceId;
    private final String targetName;
    private final long targetRef;
    private final long correlationId;
    private final SocketChannel channel;
    private final InetSocketAddress address;

    public Request(
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
}
