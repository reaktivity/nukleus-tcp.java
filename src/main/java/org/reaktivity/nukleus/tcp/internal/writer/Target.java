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
package org.reaktivity.nukleus.tcp.internal.writer;

import static java.nio.channels.SelectionKey.OP_WRITE;

import java.nio.channels.SocketChannel;
import java.util.function.ToIntFunction;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.poller.Poller;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;

@Reaktive
public final class Target implements Nukleus
{
    private final Poller poller;
    private final String targetName;

    public Target(
        Poller poller,
        String targetName)
    {
        this.poller = poller;
        this.targetName = targetName;
    }

    @Override
    public String name()
    {
        return targetName;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), targetName);
    }

    @Override
    public int process()
    {
        return 0;
    }

    public PollerKey doRegister(
        SocketChannel channel,
        ToIntFunction<PollerKey> writeHandler)
    {
        try
        {
            return poller.doRegister(channel, OP_WRITE, writeHandler);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }
}
