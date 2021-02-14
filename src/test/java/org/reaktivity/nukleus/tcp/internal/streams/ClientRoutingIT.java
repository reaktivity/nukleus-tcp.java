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
package org.reaktivity.nukleus.tcp.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

/**
 * Tests the TCP nukleus when acting as a client.
 */
public class ClientRoutingIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/tcp/streams/network/routing")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/tcp/streams/application/routing");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configurationRoot("org/reaktivity/specification/nukleus/tcp/config")
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Configuration("client.host.json")
    @Specification({
        "${app}/client.connect.with.host.extension/client",
        "${net}/client.connect.with.host.extension/server"
    })
    public void clientConnectHostExtWhenRoutedViaHost() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.subnet.json")
    @Specification({
        "${app}/client.connect.with.ipv4.extension/client",
        "${net}/client.connect.with.ipv4.extension/server"
    })
    public void shouldConnectIpv4ExtWhenRoutedViaSubnet() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.host.and.subnet.json")
    @Specification({
        "${app}/client.connect.with.ipv4.extension/client",
        "${net}/client.connect.with.ipv4.extension/server"
    })
    public void shouldConnectIpv4ExtWhenRoutedViaSubnetMultipleRoutes() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("client.subnet.ipv6.json")
    @Specification({
        "${app}/client.connect.with.ipv6.extension/client",
        "${net}/client.connect.with.ipv6.extension/server"
    })
    public void shouldConnectIpv6ExtWhenRoutedViaSubnet() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("client.host.and.subnet.ipv6.json")
    @Specification({
        "${app}/client.connect.with.ipv6.extension/client",
        "${net}/client.connect.with.ipv6.extension/server"
    })
    public void shouldConnectIpv6ExtWhenRoutedViaSubnetMultipleRoutes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.subnet.json")
    @Specification({
        "${app}/client.connect.with.host.extension/client",
        "${net}/client.connect.with.host.extension/server"
    })
    public void shouldConnectHostExtWhenRoutedViaSubnet() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.subnet.json")
    @Specification({
        "${app}/client.reset.with.no.subnet.match/client"
    })
    public void shouldResetClientWithNoSubnetMatch() throws Exception
    {
        k3po.finish();
    }
}
