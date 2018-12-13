/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.tcp.internal.TcpConfiguration.TCP_MAXIMUM_BACKLOG;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/tcp/control/unroute")
        .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .controller("tcp"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .configure(TCP_MAXIMUM_BACKLOG, 50)
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor).around(counters);

    @Test
    @Specification({
        "${route}/server/controller"
    })
    @BMRule(name = "should route server with maxmimum backlog",
            targetClass = "^java.nio.channels.ServerSocketChannel",
            targetMethod = "bind(java.net.SocketAddress, int)",
            condition = "$2 != 50",
            action = "throw new java.io.IOException(\"Unexpected backlog: \" + $2)")
    public void shouldRouteServerWithMaximumBacklog() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/error.invalid.port.0/controller"
    })
    public void shouldRefuseRouteServerPortZero() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/error.invalid.port.negative/controller"
    })
    public void shouldRefuseRouteServerNegativePort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.ip/controller"
    })
    public void shouldRouteClientIp() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/client.host/controller"
    })
    public void shouldRouteClientHost() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/client.subnet/controller"
    })
    public void shouldRouteClientSubnet() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${unroute}/server/controller"
    })
    public void shouldUnrouteServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.host/controller",
        "${unroute}/client.host/controller"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${freeze}/controller",
    })
    @ScriptProperty("nameF00C \"tcp\"")
    public void shouldFreeze() throws Exception
    {
        k3po.finish();
    }
}
