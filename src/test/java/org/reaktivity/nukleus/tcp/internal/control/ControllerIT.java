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
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.CLIENT_INITIAL;
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.CLIENT_REPLY;
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.SERVER_INITIAL;
import static org.reaktivity.nukleus.tcp.internal.router.RouteKind.SERVER_REPLY;

import java.net.InetSocketAddress;
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
        .setScriptRoot("org/reaktivity/specification/nukleus/tcp/control");

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
        "bind/client/initial/nukleus"
    })
    public void shouldBindClientInitial() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .bind(CLIENT_INITIAL.kind())
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/nukleus"
    })
    public void shouldBindClientReply() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .bind(CLIENT_REPLY.kind())
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/nukleus"
    })
    public void shouldBindServerInitial() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .bind(SERVER_INITIAL.kind())
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/nukleus"
    })
    public void shouldBindServerReply() throws Exception
    {
        k3po.start();

        controller.controller(TcpController.class)
                  .bind(SERVER_REPLY.kind())
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/nukleus",
        "unbind/initial/nukleus"
    })
    public void shouldUnbindServerInitial() throws Exception
    {
        k3po.start();

        long bindRef = controller.controller(TcpController.class)
                  .bind(SERVER_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_INITIAL");

        controller.controller(TcpController.class)
                  .unbind(bindRef)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/nukleus",
        "unbind/initial/nukleus"
    })
    public void shouldUnbindClientInitial() throws Exception
    {
        k3po.start();

        long bindRef = controller.controller(TcpController.class)
                  .bind(CLIENT_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_INITIAL");

        controller.controller(TcpController.class)
                  .unbind(bindRef)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/nukleus",
        "unbind/reply/nukleus"
    })
    public void shouldUnbindServerReply() throws Exception
    {
        k3po.start();

        long bindRef = controller.controller(TcpController.class)
                  .bind(SERVER_REPLY.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .unbind(bindRef)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/nukleus",
        "unbind/reply/nukleus"
    })
    public void shouldUnbindClientReply() throws Exception
    {
        k3po.start();

        long bindRef = controller.controller(TcpController.class)
                  .bind(CLIENT_REPLY.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .unbind(bindRef)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/nukleus",
        "route/server/initial/nukleus"
    })
    public void shouldRouteServerInitial() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = controller.controller(TcpController.class)
                  .bind(SERVER_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .route("any", sourceRef, "target", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/nukleus",
        "route/client/initial/nukleus"
    })
    public void shouldRouteClientInitial() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = controller.controller(TcpController.class)
                  .bind(CLIENT_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .route("source", sourceRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/nukleus",
        "route/server/reply/nukleus"
    })
    public void shouldRouteServerReply() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long replyRef = controller.controller(TcpController.class)
                  .bind(SERVER_REPLY.kind())
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .route("reply", replyRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/nukleus",
        "route/client/reply/nukleus"
    })
    public void shouldRouteClientReply() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long replyRef = controller.controller(TcpController.class)
                  .bind(CLIENT_REPLY.kind())
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .route("any", replyRef, "reply", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/initial/nukleus",
        "route/server/initial/nukleus",
        "unroute/server/initial/nukleus"
    })
    public void shouldUnrouteServerInitial() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = controller.controller(TcpController.class)
                  .bind(SERVER_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .route("any", sourceRef, "target", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .unroute("any", sourceRef, "target", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/initial/nukleus",
        "route/client/initial/nukleus",
        "unroute/client/initial/nukleus"
    })
    public void shouldUnrouteClientInitial() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = controller.controller(TcpController.class)
                  .bind(CLIENT_INITIAL.kind())
                  .get();

        k3po.notifyBarrier("BOUND_REPLY");

        controller.controller(TcpController.class)
                  .route("source", sourceRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .unroute("source", sourceRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/server/reply/nukleus",
        "route/server/reply/nukleus",
        "unroute/server/reply/nukleus"
    })
    public void shouldUnrouteServerReply() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long replyRef = controller.controller(TcpController.class)
                  .bind(SERVER_REPLY.kind())
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .route("reply", replyRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.notifyBarrier("ROUTED_REPLY");

        controller.controller(TcpController.class)
                  .unroute("reply", replyRef, "any", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "bind/client/reply/nukleus",
        "route/client/reply/nukleus",
        "unroute/client/reply/nukleus"
    })
    public void shouldUnrouteClientReply() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long replyRef = controller.controller(TcpController.class)
                  .bind(CLIENT_REPLY.kind())
                  .get();

        k3po.notifyBarrier("ROUTED_INITIAL");

        controller.controller(TcpController.class)
                  .route("any", replyRef, "reply", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.notifyBarrier("ROUTED_REPLY");

        controller.controller(TcpController.class)
                  .unroute("any", replyRef, "reply", targetRef, new InetSocketAddress("127.0.0.1", 8080))
                  .get();

        k3po.finish();
    }
}
