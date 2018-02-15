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
package org.reaktivity.nukleus.tcp.internal.poller;

import static org.agrona.CloseHelper.quietClose;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.agrona.LangUtil;
import org.agrona.nio.TransportPoller;
import org.reaktivity.nukleus.Nukleus;

public final class Poller extends TransportPoller implements Nukleus
{
    private final ToIntFunction<SelectionKey> selectHandler;

    public Poller()
    {
        this.selectHandler = this::handleSelect;
    }

    @Override
    public int process()
    {
        int workDone = 0;

        try
        {
            if (selector.selectNow() != 0)
            {
                workDone = selectedKeySet.forEach(selectHandler);
            }
        }
        catch (Throwable ex)
        {
            selectedKeySet.reset();
            LangUtil.rethrowUnchecked(ex);
        }

        return workDone;
    }

    @Override
    public String name()
    {
        return "poller";
    }

    @Override
    public void close()
    {
        for (SelectionKey key : selector.keys())
        {
            quietClose(key.channel());
        }

        // Allow proper cleanup on platforms like Windows
        selectNowWithoutProcessing();
    }

    public PollerKey doRegister(
        SelectableChannel channel,
        int interestOps,
        ToIntFunction<PollerKey> handler) throws ClosedChannelException
    {
        PollerKey pollerKey = null;

        try
        {
            SelectionKey key = channel.keyFor(selector);
            if (key == null)
            {
                key = channel.register(selector, interestOps, null);
                key.attach(new PollerKey(key));
            }

            pollerKey = attachment(key);

            if (handler != null)
            {
                pollerKey.handler(interestOps, handler);
            }
        }
        catch (ClosedChannelException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return pollerKey;
    }

    public Stream<PollerKey> keys()
    {
        return selector.keys().stream().map(k -> attachment(k));
    }

    private int handleSelect(
        SelectionKey key)
    {
        final PollerKey attachment = attachment(key);
        return attachment.handleSelect(key);
    }

    private static PollerKey attachment(
        SelectionKey key)
    {
        return (PollerKey) key.attachment();
    }
}
