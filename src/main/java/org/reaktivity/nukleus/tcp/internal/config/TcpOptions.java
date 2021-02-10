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
package org.reaktivity.nukleus.tcp.internal.config;

import org.reaktivity.reaktor.config.Options;

public final class TcpOptions extends Options
{
    public final String host;
    public final int port;
    public final int backlog;
    public final boolean nodelay;
    public final boolean keepalive;

    public TcpOptions(
        String host,
        int port,
        int backlog,
        boolean nodelay,
        boolean keepalive)
    {
        this.host = host;
        this.port = port;
        this.backlog = backlog;
        this.nodelay = nodelay;
        this.keepalive = keepalive;
    }
}
