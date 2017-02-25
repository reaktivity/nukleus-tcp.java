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

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

import org.agrona.LangUtil;
import org.agrona.nio.TransportPoller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;

@Reaktive
public final class Target extends TransportPoller implements Nukleus
{
    private final String targetName;
    private final ToIntFunction<SelectionKey> writeHandler;

    public Target(
        String targetName)
    {
        this.targetName = targetName;
        this.writeHandler = this::handleWrite;
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
        int weight = 0;

        try
        {
            selector.selectNow();
            weight += selectedKeySet.forEach(writeHandler);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return weight;
    }

    public SelectionKey doRegister(
        SocketChannel channel,
        int ops,
        IntSupplier attachment)
    {
        try
        {
            return channel.register(selector, ops, attachment);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }

    private int handleWrite(
        SelectionKey key)
    {
        IntSupplier supplier = (IntSupplier) key.attachment();
        return supplier.getAsInt();
    }
}
