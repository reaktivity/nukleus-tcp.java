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
package org.reaktivity.nukleus.tcp.internal.router;

import static java.util.Objects.requireNonNull;

import java.nio.channels.SocketChannel;
import java.util.Objects;

public class Correlation
{
    private final String sourceName;
    private final SocketChannel channel;

    public Correlation(
        String sourceName,
        SocketChannel channel)
    {
        this.sourceName = requireNonNull(sourceName, "sourceName");
        this.channel = requireNonNull(channel, "channel");
    }

    public String source()
    {
        return sourceName;
    }

    public SocketChannel channel()
    {
        return channel;
    }

    @Override
    public int hashCode()
    {
        int result = sourceName.hashCode();
        result = 31 * result + channel.hashCode();

        return result;
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
        return Objects.equals(this.sourceName, that.sourceName) &&
                Objects.equals(this.channel, that.channel);
    }

    @Override
    public String toString()
    {
        return String.format("[source=\"%s\", channel=%s]", sourceName, channel);
    }
}
