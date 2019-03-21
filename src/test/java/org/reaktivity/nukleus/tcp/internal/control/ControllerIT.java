/**
 * Copyright 2016-2019 The Reaktivity Project
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
import static org.junit.rules.RuleChain.outerRule;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/tcp/control/unroute")
        .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("tcp"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/server/nukleus"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.start();

        reaktor.controller(TcpController.class)
               .routeServer("tcp#0.0.0.0:8080", "target#0")
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.ip/nukleus"
    })
    public void shouldRouteClientIp() throws Exception
    {
        k3po.start();

        reaktor.controller(TcpController.class)
               .routeClient("tcp#0", "127.0.0.1:8080")
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.host/nukleus"
    })
    public void shouldRouteClientHost() throws Exception
    {
        k3po.start();

        reaktor.controller(TcpController.class)
                .routeClient("tcp#0", "localhost:8080")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.subnet/nukleus"
    })
    public void shouldRouteClientSubnet() throws Exception
    {
        k3po.start();

        reaktor.controller(TcpController.class)
                .routeClient("tcp#0", "127.0.0.1/24:8080")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServer() throws Exception
    {
        long routeId = ThreadLocalRandom.current().nextLong();

        k3po.start();

        reaktor.controller(TcpController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/client.host/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        long routeId = ThreadLocalRandom.current().nextLong();

        k3po.start();

        reaktor.controller(TcpController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/nukleus"
    })
    @ScriptProperty("nameF00N \"tcp\"")
    public void shouldFreeze() throws Exception
    {
        k3po.start();

        reaktor.controller(TcpController.class)
               .freeze()
               .get();

        k3po.finish();
    }
}
