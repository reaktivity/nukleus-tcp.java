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
package org.reaktivity.nukleus.tcp.internal.streams.rfc793;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.SocketChannelHelper;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.nukleus.tcp.internal.SocketChannelHelper.CountDownHelper;
import org.reaktivity.reaktor.test.ReaktorRule;

/**
 * Tests use of the nukleus as an HTTP server.
 */
@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
public class ServerResetAndAbortIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/tcp/rfc793")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/tcp/streams/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .controller(TcpController.class::isAssignableFrom)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(SocketChannelHelper.RULE)
            .around(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.sent.abort/server"
    })
    public void shouldShutdownOutputWhenServerSendsAbort() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(20);
            int len = channel.read(buf);
            assertEquals(-1, len);
        }
        finally
        {
            k3po.finish();
        }
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.sent.abort.and.reset/server"
    })
    @BMRule(name = "shutdownInput",
    targetClass = "^java.nio.channels.SocketChannel",
    targetMethod = "shutdownInput()",
    helper = "org.reaktivity.nukleus.tcp.internal.SocketChannelHelper$CountDownHelper",
    condition =
      "callerMatches(\"org.reaktivity.nukleus.tcp.internal.stream.ReadStream..*\", true, true)",
      action = "countDown()"
    )
    public void shouldShutdownOutputAndInputWhenServerSendsAbortAndReset() throws Exception
    {
        CountDownLatch shutdownInputCalled = new CountDownLatch(1);
        CountDownHelper.initialize(shutdownInputCalled);
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(20);
            int len = channel.read(buf);
            assertEquals(-1, len);
            shutdownInputCalled.await();
        }
        finally
        {
            k3po.finish();
        }
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.sent.reset.and.abort/server"
    })
    @BMRule(name = "shutdownInput",
    targetClass = "^java.nio.channels.SocketChannel",
    targetMethod = "shutdownInput()",
    helper = "org.reaktivity.nukleus.tcp.internal.SocketChannelHelper$CountDownHelper",
    condition =
      "callerMatches(\"org.reaktivity.nukleus.tcp.internal.stream.ReadStream..*\", true, true)",
      action = "countDown()"
    )
    public void shouldShutdownOutputAndInputWhenServerSendsResetAndAbort() throws Exception
    {
        CountDownLatch shutdownInputCalled = new CountDownLatch(1);
        CountDownHelper.initialize(shutdownInputCalled);
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(20);
            int len = channel.read(buf);
            assertEquals(-1, len);
            shutdownInputCalled.await();
        }
        finally
        {
            k3po.finish();
        }
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.sent.reset/server"
    })
    @BMRule(name = "shutdownInput",
    targetClass = "^java.nio.channels.SocketChannel",
    targetMethod = "shutdownInput()",
    helper = "org.reaktivity.nukleus.tcp.internal.SocketChannelHelper$CountDownHelper",
    condition =
      "callerMatches(\"org.reaktivity.nukleus.tcp.internal.stream.ReadStream..*\", true, true)",
      action = "countDown()"
    )
    public void shouldShutdownInputWhenServerSendsReset() throws Exception
    {
        CountDownLatch shutdownInputCalled = new CountDownLatch(1);
        CountDownHelper.initialize(shutdownInputCalled);
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            channel.configureBlocking(false);

            channel.write(ByteBuffer.wrap("some data".getBytes()));

            try
            {
                k3po.awaitBarrier("READ_ABORTED");
                shutdownInputCalled.await();
            }
            catch(IOException e)
            {
                throw e;
            }
        }
        finally
        {
            k3po.finish();
        }
    }



}
