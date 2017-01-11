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
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
        .setScriptRoot("org/reaktivity/specification/nukleus/tcp/control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("tcp")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(nukleus);

    @Test
    @Specification({
        "bind/client/initial/controller"
    })
    public void shouldBindClientInitial() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/controller"
    })
    public void shouldBindClientReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/controller"
    })
    public void shouldBindServerInitial() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/controller"
    })
    public void shouldBindServerReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/controller",
        "unbind/initial/controller"
    })
    public void shouldUnbindServerInitial() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/controller",
        "unbind/initial/controller"
    })
    public void shouldUnbindClientInitial() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/controller",
        "unbind/reply/controller"
    })
    public void shouldUnbindServerReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/controller",
        "unbind/reply/controller"
    })
    public void shouldUnbindClientReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/controller",
        "route/server/initial/controller"
    })
    public void shouldRouteServerInitial() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("BOUND_INITIAL");
        k3po.notifyBarrier("BOUND_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/controller",
        "route/client/initial/controller"
    })
    public void shouldRouteClientInitial() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("BOUND_INITIAL");
        k3po.notifyBarrier("BOUND_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/controller",
        "route/server/reply/controller"
    })
    public void shouldRouteServerReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.awaitBarrier("BOUND_REPLY");
        k3po.notifyBarrier("ROUTED_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/controller",
        "route/client/reply/controller"
    })
    public void shouldRouteClientReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.awaitBarrier("BOUND_REPLY");
        k3po.notifyBarrier("ROUTED_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/controller",
        "route/server/initial/controller",
        "unroute/server/initial/controller"
    })
    public void shouldUnrouteServerInitial() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("BOUND_INITIAL");
        k3po.notifyBarrier("BOUND_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/controller",
        "route/client/initial/controller",
        "unroute/client/initial/controller"
    })
    public void shouldUnrouteClientInitial() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("BOUND_INITIAL");
        k3po.notifyBarrier("BOUND_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/controller",
        "route/server/reply/controller",
        "unroute/server/reply/controller"
    })
    public void shouldUnrouteServerReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.awaitBarrier("BOUND_REPLY");
        k3po.notifyBarrier("ROUTED_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/controller",
        "route/client/reply/controller",
        "unroute/client/reply/controller"
    })
    public void shouldUnrouteClientReply() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("BOUND_INITIAL");
        k3po.awaitBarrier("BOUND_REPLY");
        k3po.notifyBarrier("ROUTED_INITIAL");
        k3po.finish();
    }
}
