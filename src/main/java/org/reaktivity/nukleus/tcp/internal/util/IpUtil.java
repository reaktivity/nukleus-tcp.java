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
package org.reaktivity.nukleus.tcp.internal.util;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.TcpAddressFW;

public final class IpUtil
{
    private static final int FIELD_SIZE_IPV4_ADDRESS = 4;
    private static final int FIELD_SIZE_IPV6_ADDRESS = 16;

    // TODO: thread safety
    private static final byte[] IPV4_ADDRESS_BYTES = new byte[FIELD_SIZE_IPV4_ADDRESS];
    private static final byte[] IPV6_ADDRESS_BYTES = new byte[FIELD_SIZE_IPV6_ADDRESS];

    private static final InetAddress UNREACHABLE = null;

    private IpUtil()
    {
        // no instances
    }

    public static InetAddress inetAddress(
        TcpAddressFW address)
    {
        if (address.length() == 0)
        {
            return null;
        }

        switch (address.kind())
        {
        case TcpAddressFW.KIND_IPV4_ADDRESS:
            return address.ipv4Address().get(IpUtil::ipv4Address);
        case TcpAddressFW.KIND_IPV6_ADDRESS:
            return address.ipv6Address().get(IpUtil::ipv6Address);
        default:
            throw new IllegalStateException("Unrecognized kind: " + address.kind());
        }
    }

    public static void socketAddress(
        InetSocketAddress ipAddress,
        Consumer<Consumer<OctetsFW.Builder>> ipv4Address,
        Consumer<Consumer<OctetsFW.Builder>> ipv6Address)
    {
        inetAddress(ipAddress.getAddress(), ipv4Address, ipv6Address);
    }

    public static void inetAddress(
        InetAddress inetAddress,
        Consumer<Consumer<OctetsFW.Builder>> ipv4Address,
        Consumer<Consumer<OctetsFW.Builder>> ipv6Address)
    {
        if (inetAddress instanceof Inet4Address)
        {
            ipv4Address.accept(o -> o.set(inetAddress.getAddress()));
        }
        else if (inetAddress instanceof Inet6Address)
        {
            ipv6Address.accept(o -> o.set(inetAddress.getAddress()));
        }
    }

    public static InetAddress ipv4Address(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        try
        {
            assert limit - offset == IPV4_ADDRESS_BYTES.length;
            buffer.getBytes(offset, IPV4_ADDRESS_BYTES, 0, IPV4_ADDRESS_BYTES.length);
            return InetAddress.getByAddress(IPV4_ADDRESS_BYTES);
        }
        catch (UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return UNREACHABLE;
        }
    }

    public static InetAddress ipv6Address(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        try
        {
            assert limit - offset == IPV6_ADDRESS_BYTES.length;
            buffer.getBytes(offset, IPV6_ADDRESS_BYTES, 0, IPV6_ADDRESS_BYTES.length);
            return InetAddress.getByAddress(IPV6_ADDRESS_BYTES);
        }
        catch (UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return UNREACHABLE;
        }
    }

    public static String describe(
        InetSocketAddress localAddress)
    {
        return String.format("local.address == %s", localAddress);
    }

}
