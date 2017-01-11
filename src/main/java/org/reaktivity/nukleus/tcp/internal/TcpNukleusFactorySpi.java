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
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.tcp.internal.acceptor.Acceptor;
import org.reaktivity.nukleus.tcp.internal.conductor.Conductor;
import org.reaktivity.nukleus.tcp.internal.connector.Connector;
import org.reaktivity.nukleus.tcp.internal.router.Router;
import org.reaktivity.nukleus.tcp.internal.watcher.Watcher;

public final class TcpNukleusFactorySpi implements NukleusFactorySpi
{
    @Override
    public String name()
    {
        return TcpNukleus.NAME;
    }

    @Override
    public TcpNukleus create(Configuration config)
    {
        Context context = new Context();
        context.conclude(config);

        Conductor conductor = new Conductor(context);
        Router router = new Router(context);
        Watcher watcher = new Watcher(context);
        Acceptor acceptor = new Acceptor();
        Connector connector = new Connector(context);

        router.setConductor(conductor);
        acceptor.setConductor(conductor);

        router.setAcceptor(acceptor);
        router.setConnector(connector);

        watcher.setRouter(router);
        conductor.setRouter(router);
        acceptor.setRouter(router);
        connector.setRouter(router);

        return new TcpNukleus(conductor, router, watcher, acceptor, connector, context);
    }
}
