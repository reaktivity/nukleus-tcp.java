/**
 * Copyright 2016-2020 The Reaktivity Project
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
    public static final IntPropertyDef TCP_MAXIMUM_BACKLOG;
    public static final IntPropertyDef TCP_WINDOW_THRESHOLD;
    public static final IntPropertyDef TCP_MAX_CONNECTIONS;
    public static final BooleanPropertyDef TCP_KEEPALIVE;
    public static final BooleanPropertyDef TCP_NODELAY;

    private static final ConfigurationDef TCP_CONFIG;

    static
    {
        ConfigurationDef config = new ConfigurationDef("nukleus.tcp");
        TCP_MAXIMUM_BACKLOG = config.property("maximum.backlog", 0);
        TCP_WINDOW_THRESHOLD = config.property("window.threshold", 0);
        TCP_MAX_CONNECTIONS = config.property("max.connections", Integer.MAX_VALUE);
        TCP_KEEPALIVE = config.property("keepalive", false);
        TCP_NODELAY = config.property("nodelay", true);
        TCP_CONFIG = config;
    }

    public TcpConfiguration(
        Configuration config)
    {
        super(TCP_CONFIG, config);
    }

    /**
     * @see java.nio.channels.ServerSocketChannel#bind(java.net.SocketAddress, int)
     */
    public int maximumBacklog()
    {
        return TCP_MAXIMUM_BACKLOG.getAsInt(this);
    }

    // accumulates window until the threshold and sends it once it reaches over the threshold
    public int windowThreshold()
    {
        int threshold = TCP_WINDOW_THRESHOLD.getAsInt(this);
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
        return TCP_MAX_CONNECTIONS.getAsInt(this);
    }

    public boolean keepalive()
    {
        return TCP_KEEPALIVE.getAsBoolean(this);
    }

    public boolean nodelay()
    {
        return TCP_NODELAY.getAsBoolean(this);
    }

}
