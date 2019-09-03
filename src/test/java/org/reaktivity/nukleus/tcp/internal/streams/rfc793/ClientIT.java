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
package org.reaktivity.nukleus.tcp.internal.streams.rfc793;

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

/**
 * Tests the TCP nukleus when acting as a client.
 */
public class ClientIT
{
    private static final long CLIENT_ROUTE_ID = 0x1000210000001L;

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/tcp/rfc793")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/tcp/streams/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client.host/controller",
        "${client}/client.and.server.sent.data.multiple.frames/client",
        "${server}/client.and.server.sent.data.multiple.frames/server"
    })
    public void shouldSendAndReceiveData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}client.host/controller",
            "${client}/client.and.server.sent.data.with.padding/client",
            "${server}/client.and.server.sent.data.with.padding/server"
    })
    public void shouldSendAndReceiveDataWithPadding() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.close/client",
        "${server}/client.close/server"
    })
    public void shouldInitiateClientClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.sent.data/client",
        "${server}/client.sent.data/server"
    })
    public void shouldReceiveClientSentData() throws Exception
    {
       k3po.finish();
       assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.sent.data.multiple.frames/client",
        "${server}/client.sent.data.multiple.frames/server"
    })
    public void shouldReceiveClientSentDataMultipleFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.sent.data.multiple.streams/client",
        "${server}/client.sent.data.multiple.streams/server"
    })
    public void shouldReceiveClientSentDataMultipleStreams() throws Exception
    {
        k3po.finish();
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.sent.data.then.end/client"
        // No support for "read closed" in k3po tcp
    })
    public void shouldReceiveClientSentDataAndEnd() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                ByteBuffer buf = ByteBuffer.allocate(256);
                channel.read(buf);
                buf.flip();
                assertEquals("client data", UTF_8.decode(buf).toString());

                buf.rewind();
                int len = channel.read(buf);

                assertEquals(-1, len);

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/client.sent.end.then.received.data/client"
        // No support for "read closed" in k3po tcp
    })
    public void shouldWriteDataAfterReceivingEndOfRead() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                ByteBuffer buf = ByteBuffer.allocate(256);
                int len = channel.read(buf);

                assertEquals(-1, len);

                channel.write(UTF_8.encode("server data"));

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/connection.established/client",
        "${server}/connection.established/server"
    })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/connection.failed/client"
    })
    public void connnectionFailed() throws Exception
    {
        k3po.finish();
        Thread.sleep(250); // TODO: reaktor quiese instead of close
        assertEquals(1, reaktor.resetsRead("tcp", CLIENT_ROUTE_ID));
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.close/client",
        "${server}/server.close/server"
    })
    public void shouldInitiateServerClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.data/client",
        "${server}/server.sent.data/server"
    })
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.finish();

        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.data/client",
        "${server}/server.sent.data/server"
    })
    @ScriptProperty("clientInitialWindow \"6\"")
    public void shouldReceiveServerSentDataWithFlowControl() throws Exception
    {
        k3po.finish();

        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.data.multiple.frames/client",
        "${server}/server.sent.data.multiple.frames/server"
    })
    public void shouldReceiveServerSentDataMultipleFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.data.multiple.streams/client",
        "${server}/server.sent.data.multiple.streams/server"
    })
    public void shouldReceiveServerSentDataMultipleStreams() throws Exception
    {
        k3po.finish();

        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.data.then.end/client"
        // No support for half close output in k3po tcp
    })
    public void shouldReceiveServerSentDataAndEnd() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                channel.write(UTF_8.encode("server data"));

                channel.shutdownOutput();

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}client.host/controller",
        "${client}/server.sent.end.then.received.data/client"
        // No support for "write close" in k3po tcp
    })
    public void shouldWriteDataAfterReceiveEnd() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                channel.shutdownOutput();

                ByteBuffer buf = ByteBuffer.allocate(256);
                channel.read(buf);
                buf.flip();

                assertEquals("client data", UTF_8.decode(buf).toString());

                k3po.finish();
            }
        }
    }
}
