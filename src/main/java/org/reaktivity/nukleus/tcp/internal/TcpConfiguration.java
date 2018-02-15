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

    public static final String TRANSFER_CAPACITY_PROPERTY_NAME = "nukleus.tcp.transfer.capacity";

    public static final String PENDING_REGIONS_CAPACITY_PROPERTY_NAME = "nukleus.tcp.pending.regions.capacity";

    /**
     * @see java.nio.channels.ServerSocketChannel#bind(java.net.SocketAddress, int)
     */
    private static final int MAXIMUM_BACKLOG_DEFAULT = 0;

    private static final int TRANSFER_CAPACITY_DEFAULT = 1 << 15;

    private static final int PENDING_REGIONS_CAPACITY_DEFAULT = 1 << 10;

    public TcpConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int maximumBacklog()
    {
        return getInteger(MAXIMUM_BACKLOG_PROPERTY_NAME, MAXIMUM_BACKLOG_DEFAULT);
    }

    public int transferCapacity()
    {
        return getInteger(TRANSFER_CAPACITY_PROPERTY_NAME, TRANSFER_CAPACITY_DEFAULT);
    }

    public int pendingRegionsCapacity()
    {
        return getInteger(PENDING_REGIONS_CAPACITY_PROPERTY_NAME, PENDING_REGIONS_CAPACITY_DEFAULT);
    }
}
