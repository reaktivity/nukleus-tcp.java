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
import static org.reaktivity.nukleus.tcp.internal.types.control.Role.INPUT;
import static org.reaktivity.nukleus.tcp.internal.types.control.Role.OUTPUT;
import static org.reaktivity.nukleus.tcp.internal.types.control.State.ESTABLISHED;
import static org.reaktivity.nukleus.tcp.internal.types.control.State.NEW;

import java.net.InetAddress;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.reaktor.test.ControllerRule;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/tcp/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ControllerRule controller = new ControllerRule(TcpController.class)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(controller);

    @Test
    @Specification({
        "${route}/input/new/nukleus"
    })
    public void shouldRouteInputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        InetAddress address = InetAddress.getByName("127.0.0.1");

        k3po.start();

        controller.controller(TcpController.class)
                  .route(INPUT, NEW, "any", 8080, "target", targetRef, address)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/nukleus"
    })
    public void shouldRouteOutputNew() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .route(OUTPUT, NEW, "source", 0L, "localhost", 8080, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/established/nukleus"
    })
    public void shouldRouteOutputEstablished() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .route(OUTPUT, ESTABLISHED, "target", 0L, "any", 0L, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/established/nukleus"
    })
    public void shouldRouteInputEstablished() throws Exception
    {
        long sourceRef = new Random().nextLong();

        k3po.start();

        controller.controller(TcpController.class)
                  .route(INPUT, ESTABLISHED, "any", 8080, "source", sourceRef, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/input/new/nukleus"
    })
    public void shouldUnrouteInputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        InetAddress address = InetAddress.getByName("127.0.0.1");

        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");

        controller.controller(TcpController.class)
                  .unroute(INPUT, NEW, "any", 8080, "target", targetRef, address)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/output/new/nukleus"
    })
    public void shouldUnrouteOutputNew() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_OUTPUT");

        long sourceRef = new Random().nextLong();

        controller.controller(TcpController.class)
                  .unroute(OUTPUT, NEW, "source", sourceRef, "localhost", 8080, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/output/established/nukleus"
    })
    public void shouldUnrouteOutputEstablished() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();
        k3po.notifyBarrier("ROUTED_OUTPUT");

        controller.controller(TcpController.class)
                  .unroute(OUTPUT, ESTABLISHED, "target", targetRef, "any", 0L, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/input/established/nukleus"
    })
    public void shouldUnrouteInputEstablished() throws Exception
    {
        long sourceRef = new Random().nextLong();

        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");

        controller.controller(TcpController.class)
                  .unroute(INPUT, ESTABLISHED, "any", 8080, "source", sourceRef, null)
                  .get();

        k3po.finish();
    }
}
