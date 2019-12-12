/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.poller;

import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.function.ToIntFunction;

public final class PollerKey
{
    private ToIntFunction<PollerKey> acceptHandler = PollerKey::nop;
    private ToIntFunction<PollerKey> connectHandler = PollerKey::nop;
    private ToIntFunction<PollerKey> readHandler = PollerKey::nop;
    private ToIntFunction<PollerKey> writeHandler = PollerKey::nop;

    private final SelectionKey key;
    private int interestOps;

    PollerKey(
        SelectionKey key)
    {
        this.key = key;
        this.interestOps = key.interestOps();
    }

    public SelectableChannel channel()
    {
        return key.channel();
    }

    public boolean isValid()
    {
        return key.isValid();
    }

    public void register(
        int registerOps)
    {
        final int newInterestOps = interestOps | registerOps;
        if (newInterestOps != interestOps)
        {
            key.interestOps(newInterestOps);
            interestOps = newInterestOps;
        }
    }

    public void clear(
        int clearOps)
    {
        final int newInterestOps = interestOps & ~clearOps;
        if (newInterestOps != interestOps)
        {
            key.interestOps(newInterestOps);
            interestOps = newInterestOps;
        }
    }

    public void handler(
        final int handlerOps,
        final ToIntFunction<PollerKey> handler)
    {
        if ((handlerOps & OP_ACCEPT) != 0)
        {
            acceptHandler = (handler != null) ? handler : PollerKey::nop;
        }

        if ((handlerOps & OP_CONNECT) != 0)
        {
            connectHandler = (handler != null) ? handler : PollerKey::nop;
        }

        if ((handlerOps & OP_READ) != 0)
        {
            readHandler = (handler != null) ? handler : PollerKey::nop;
        }

        if ((handlerOps & OP_WRITE) != 0)
        {
            writeHandler = (handler != null) ? handler : PollerKey::nop;
        }
    }

    int handleSelect(
        SelectionKey key)
    {
        // guarantee ready set matches interest ops, see SelectionKey
        final int readyOps = key.readyOps() & this.interestOps;

        int workDone = 0;

        if ((readyOps & SelectionKey.OP_ACCEPT) != 0)
        {
            workDone += acceptHandler.applyAsInt(this);
        }

        if ((readyOps & SelectionKey.OP_CONNECT) != 0)
        {
            workDone += connectHandler.applyAsInt(this);
        }

        if ((readyOps & SelectionKey.OP_READ) != 0)
        {
            workDone += readHandler.applyAsInt(this);
        }

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
        {
            workDone += writeHandler.applyAsInt(this);
        }

        return workDone;
    }

    private static int nop(
        PollerKey key)
    {
        return 0;
    }
}
