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
package org.reaktivity.nukleus.tcp.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/tcp/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("tcp")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    private final TcpCountersRule counters = new TcpCountersRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(nukleus).around(counters);

    @Test
    @Specification({
        "${route}/input/none/controller"
    })
    public void shouldRouteInputNone() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller"
    })
    public void shouldRouteInputNew() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/established/controller"
    })
    public void shouldRouteInputEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/none/controller"
    })
    public void shouldRouteOutputNone() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/output/new/controller"
    })
    public void shouldRouteOutputNew() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/output/established/controller"
    })
    public void shouldRouteOutputEstablished() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/input/none/controller",
        "${unroute}/input/none/controller"
    })
    public void shouldUnrouteInputNone() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${unroute}/input/new/controller"
    })
    public void shouldUnrouteInputNew() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/established/controller",
        "${unroute}/input/established/controller"
    })
    public void shouldUnrouteInputEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/none/controller",
        "${unroute}/output/none/controller"
    })
    public void shouldUnrouteOutputNone() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${unroute}/output/new/controller"
    })
    public void shouldUnrouteOutputNew() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }

    @Test
    @Specification({
        "${route}/output/established/controller",
        "${unroute}/output/established/controller"
    })
    public void shouldUnrouteOutputEstablished() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.routes());
    }
}
