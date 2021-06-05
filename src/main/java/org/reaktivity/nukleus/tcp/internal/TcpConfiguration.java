/**
 * Copyright 2016-2021 The Reaktivity Project
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

import org.reaktivity.reaktor.nukleus.Configuration;

public class TcpConfiguration extends Configuration
{
    public static final IntPropertyDef TCP_WINDOW_THRESHOLD;
    public static final IntPropertyDef TCP_MAX_CONNECTIONS;

    private static final ConfigurationDef TCP_CONFIG;

    static
    {
        ConfigurationDef config = new ConfigurationDef("nukleus.tcp");
        TCP_WINDOW_THRESHOLD = config.property("window.threshold", 0);
        TCP_MAX_CONNECTIONS = config.property("max.connections", Integer.MAX_VALUE);
        TCP_CONFIG = config;
    }

    public TcpConfiguration(
        Configuration config)
    {
        super(TCP_CONFIG, config);
    }

    public int windowThreshold()
    {
        int threshold = TCP_WINDOW_THRESHOLD.getAsInt(this);
        assert threshold >= 0 && threshold <= 100;
        return threshold;
    }

    public int maxConnections()
    {
        return TCP_MAX_CONNECTIONS.getAsInt(this);
    }
}
