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
package org.reaktivity.nukleus.tcp.internal.stream;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.Address;
import org.reaktivity.nukleus.route.AddressFactory;
import org.reaktivity.nukleus.tcp.internal.TcpNukleus;

public class TcpAddressFactory implements AddressFactory
{
    private final MessageConsumer routeHandler;

    public TcpAddressFactory(
        MessageConsumer routeHandler)
    {
        this.routeHandler = routeHandler;
    }

    @Override
    public TcpAddress newAddress(
        String name)
    {
        return new TcpAddress(name);
    }

    private final class TcpAddress implements Address
    {
        private final String name;

        TcpAddress(
            String name)
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public String nukleus()
        {
            return TcpNukleus.NAME;
        }

        @Override
        public MessageConsumer routeHandler()
        {
            return routeHandler;
        }
    }
}
