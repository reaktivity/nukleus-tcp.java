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
package org.reaktivity.nukleus.tcp.internal;

import org.reaktivity.nukleus.Configuration;

public class TcpConfiguration extends Configuration
{
    public static final String MAXIMUM_BACKLOG_PROPERTY_NAME = "nukleus.tcp.maximum.backlog";
    public static final String WINDOW_THRESHOLD_PROPERTY_NAME = "nukleus.tcp.window.threshold";
    public static final String MAX_CONNECTIONS_NAME = "nukleus.tcp.max.connections";
    public static final String TCP_KEEP_ALIVE_NAME = "nukleus.tcp.keepalive";

    /**
     * @see java.nio.channels.ServerSocketChannel#bind(java.net.SocketAddress, int)
     */
    private static final int MAXIMUM_BACKLOG_DEFAULT = 0;

    private static final int WINDOW_THRESHOLD_DEFAULT = 0;

    private static final int MAX_CONNECTIONS_DEFAULT = Integer.MAX_VALUE;

    private static final boolean TCP_KEEP_ALIVE_DEFAULT = false;

    public TcpConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int maximumBacklog()
    {
        return getInteger(MAXIMUM_BACKLOG_PROPERTY_NAME, MAXIMUM_BACKLOG_DEFAULT);
    }

    // Accumulates window until the threshold and sends it once it reaches over the threshold
    // @return window's threshold
    public int windowThreshold()
    {
        int threshold = getInteger(WINDOW_THRESHOLD_PROPERTY_NAME, WINDOW_THRESHOLD_DEFAULT);
        if (threshold < 0 || threshold > 100)
        {
            String message = String.format(
                    "TCP write window threshold is %d (should be between 0 and 100 inclusive)", threshold);
            throw new IllegalArgumentException(message);
        }
        return threshold;
    }

    public int maxConnections()
    {
        return getInteger(MAX_CONNECTIONS_NAME, MAX_CONNECTIONS_DEFAULT);
    }

    public boolean tcpKeepalive()
    {
        return getBoolean(TCP_KEEP_ALIVE_NAME, TCP_KEEP_ALIVE_DEFAULT);
    }

}
