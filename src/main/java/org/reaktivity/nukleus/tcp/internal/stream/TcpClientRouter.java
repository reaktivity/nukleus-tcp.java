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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.tcp.internal.config.TcpBinding;
import org.reaktivity.nukleus.tcp.internal.config.TcpOptions;
import org.reaktivity.nukleus.tcp.internal.config.TcpRoute;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressFW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInet4FW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInet6FW;
import org.reaktivity.nukleus.tcp.internal.types.ProxyAddressInetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.reaktor.nukleus.ElektronContext;

public final class TcpClientRouter
{
    private final byte[] ipv4RO = new byte[4];
    private final byte[] ipv6ros = new byte[16];

    private final Function<String, InetAddress[]> resolveHost;
    private final Long2ObjectHashMap<TcpBinding> bindings;

    public TcpClientRouter(
        ElektronContext context)
    {
        this.resolveHost = context::resolveHost;
        this.bindings = new Long2ObjectHashMap<>();
    }

    public void attach(
        TcpBinding binding)
    {
        bindings.put(binding.routeId, binding);
    }

    public InetSocketAddress resolve(
        long routeId,
        long authorization,
        ProxyBeginExFW beginEx)
    {
        InetSocketAddress resolved = null;

        TcpBinding binding = bindings.get(routeId);
        if (binding != null)
        {
            resolved = resolve(binding, authorization, beginEx);
        }

        return resolved;
    }

    public void detach(
        long routeId)
    {
        bindings.remove(routeId);
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), bindings);
    }

    private InetSocketAddress resolve(
        TcpBinding binding,
        long authorization,
        ProxyBeginExFW beginEx)
    {
        InetSocketAddress resolved = null;

        if (beginEx == null)
        {
            final TcpOptions options = binding.options;
            resolved = new InetSocketAddress(options.host, options.port);
        }
        else
        {
            final ProxyAddressFW address = beginEx.address();

            subnet:
            for (TcpRoute route : binding.routes)
            {
                try
                {
                    Predicate<? super InetAddress> matchSubnet =
                        a -> route.when.stream().filter(m -> m.matches(a)).findFirst().isPresent();

                    switch (address.kind())
                    {
                    case INET:
                        ProxyAddressInetFW addressInet = address.inet();
                        resolved = resolveInet(addressInet, matchSubnet);
                        break subnet;
                    case INET4:
                        ProxyAddressInet4FW addressInet4 = address.inet4();
                        resolved = resolveInet4(addressInet4, matchSubnet);
                        break subnet;
                    case INET6:
                        ProxyAddressInet6FW addressInet6 = address.inet6();
                        resolved = resolveInet6(addressInet6, matchSubnet);
                        break subnet;
                    default:
                        throw new RuntimeException("Unexpected address kind");
                    }
                }
                catch (UnknownHostException ex)
                {
                   // ignore
                }
            }
        }

        return resolved;
    }

    private InetSocketAddress resolveInet6(
        ProxyAddressInet6FW address,
        Predicate<? super InetAddress> filter) throws UnknownHostException
    {
        OctetsFW destination = address.destination();
        int destinationPort = address.destinationPort();

        byte[] ipv6 = ipv6ros;
        destination.buffer().getBytes(destination.offset(), ipv6);

        return Optional
                .of(InetAddress.getByAddress(ipv6))
                .filter(filter)
                .map(a -> new InetSocketAddress(a, destinationPort))
                .orElse(null);
    }

    private InetSocketAddress resolveInet(
        ProxyAddressInetFW address,
        Predicate<? super InetAddress> filter)
    {
        return Arrays
                .stream(resolveHost.apply(address.destination().asString()))
                .filter(filter)
                .findFirst()
                .map(a -> new InetSocketAddress(a, address.destinationPort()))
                .orElse(null);
    }

    private InetSocketAddress resolveInet4(
        ProxyAddressInet4FW address,
        Predicate<? super InetAddress> filter) throws UnknownHostException
    {
        OctetsFW destination = address.destination();
        int destinationPort = address.destinationPort();

        byte[] ipv4 = ipv4RO;
        destination.buffer().getBytes(destination.offset(), ipv4);

        return Optional
                .of(InetAddress.getByAddress(ipv4))
                .filter(filter)
                .map(a -> new InetSocketAddress(a, destinationPort))
                .orElse(null);
    }
}
