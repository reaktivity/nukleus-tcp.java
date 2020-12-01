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
package org.reaktivity.nukleus.tcp.internal.streams.rfc793;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

/**
 * Tests the TCP nukleus when acting as a client.
 */
public class ClientRoutingIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/tcp/streams/network/routing")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/tcp/streams/application/routing");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client.host/controller",
        "${client}/client.connect.with.host.extension/client",
        "${server}/client.connect.with.host.extension/server"
    })
    public void clientConnectHostExtWhenRoutedViaHost() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.subnet/controller",
        "${client}/client.connect.with.ip.extension/client",
        "${server}/client.connect.with.ip.extension/server"
    })
    public void shouldConnectIpExtWhenRoutedViaSubnet() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.host.and.subnet/controller",
        "${client}/client.connect.with.ip.extension/client",
        "${server}/client.connect.with.ip.extension/server"
    })
    public void shouldConnectIpExtWhenRoutedViaSubnetMultipleRoutes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.subnet/controller",
        "${client}/client.connect.with.host.extension/client",
        "${server}/client.connect.with.host.extension/server"
    })
    public void shouldConnectHostExtWhenRoutedViaSubnet() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.subnet/controller",
        "${client}/client.reset.with.no.subnet.match/client"
    })
    public void shouldResetClientWithNoSubnetMatch() throws Exception
    {
        k3po.finish();
    }

}
