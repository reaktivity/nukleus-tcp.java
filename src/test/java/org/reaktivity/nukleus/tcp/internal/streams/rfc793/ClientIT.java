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

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.nukleus.tcp.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.test.ReaktorRule;

/**
 * Tests use of the nukleus as an HTTP client.
 */
public class ClientIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/tcp/rfc793")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/tcp/streams/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .controller(TcpController.class::isAssignableFrom)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .configure("reaktor.abort.stream.frame.type.id", AbortFW.TYPE_ID);

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/client.sent.abort/client"
    })
    public void shouldShutdownOutputWhenClientSendsAbort() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                ByteBuffer buf = ByteBuffer.allocate(20);
                int len = channel.read(buf);
                assertEquals(-1, len);
            }
            finally
            {
                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/client.sent.abort.and.reset/client"
    })
    public void shouldShutdownOutputAndInputWhenClientSendsAbortAndReset() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                ByteBuffer buf = ByteBuffer.allocate(20);
                int len = channel.read(buf);
                assertEquals(-1, len);
            }
            finally
            {
                k3po.finish();
            }
        }
    }

    @Test(expected = IOException.class)
    @Specification({
        "${route}/client/controller",
        "${client}/client.sent.reset/client"
    })
    public void shouldShutdownInputWhenClientSendsReset() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                channel.configureBlocking(false);

                channel.write(ByteBuffer.wrap("some data".getBytes()));

                ByteBuffer buf = ByteBuffer.allocate(20);
                try
                {
                    k3po.awaitBarrier("READ_ABORTED");

                    int len;
                    // Send more data, this should cause other end to send TCP RST if input was shutdown
                    // Depending on timing we may need to repeat this operation till we get the IOException
                    do
                    {
                        channel.write(ByteBuffer.wrap("more data".getBytes()));
                        len = channel.read(buf);
                    }
                    while (len == 0);
                    fail(String.format("channel.read returned %d instead of throwing IOException", len));
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


}
