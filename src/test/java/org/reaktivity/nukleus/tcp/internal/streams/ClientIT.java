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
package org.reaktivity.nukleus.tcp.internal.streams;

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
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
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class ClientIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/tcp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("tcp")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .streams("tcp", "source#partition");

    private final TcpCountersRule counters = new TcpCountersRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/connection.established/client/source"
    })
    public void shouldEstablishConnection() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");
                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data/client/source"
    })
    public void shouldReceiveServerSentData() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel.write(UTF_8.encode("server data"));

                k3po.finish();
            }
        }

        assertEquals(1, counters.streams());
        assertEquals(1, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.overflow/client/source"
    })
    public void shouldReceiveServerSentDataWithFlowControl() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel.write(UTF_8.encode("server data"));

                k3po.finish();
            }
        }

        assertEquals(1, counters.streams());
        assertEquals(1, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.multiple.frames/client/source"
    })
    public void shouldReceiveServerSentDataMultipleFrames() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");
                channel.write(UTF_8.encode("server data 1"));

                k3po.awaitBarrier("FIRST_DATA_FRAME_RECEIVED");
                channel.write(UTF_8.encode("server data 2"));

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.multiple.streams/client/source"
    })
    public void shouldReceiveServerSentDataMultipleStreams() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel1 = server.accept();
                 SocketChannel channel2 = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel1.write(UTF_8.encode("server data 1"));

                channel2.write(UTF_8.encode("server data 2"));

                k3po.finish();
            }
        }

        assertEquals(2, counters.streams());
        assertEquals(1, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.then.end/client/source"
    })
    public void shouldReceiveServerSentDataAndEnd() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel.write(UTF_8.encode("server data"));

                channel.shutdownOutput();

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data/client/source"
    })
    public void shouldReceiveClientSentData() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                ByteBuffer buf = ByteBuffer.allocate(256);
                channel.read(buf);
                buf.flip();

                assertEquals("client data", UTF_8.decode(buf).toString());

                k3po.finish();
            }
        }

        assertEquals(1, counters.streams());
        assertEquals(1, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.multiple.frames/client/source"
    })
    public void shouldReceiveClientSentDataMultipleFrames() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                ByteBuffer buf = ByteBuffer.allocate(256);
                while (channel.read(buf) != -1 && buf.position() < 26)
                {
                }
                buf.flip();

                assertEquals("client data 1".concat("client data 2"), UTF_8.decode(buf).toString());
            }
            finally
            {
                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.multiple.streams/client/source"
    })
    public void shouldReceiveClientSentDataMultipleStreams() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel1 = server.accept();
                 SocketChannel channel2 = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                ByteBuffer buf = ByteBuffer.allocate(256);
                channel1.read(buf);
                buf.flip();
                assertEquals("client data 1", UTF_8.decode(buf).toString());

                buf.rewind();
                channel2.read(buf);
                buf.flip();
                assertEquals("client data 2", UTF_8.decode(buf).toString());

                k3po.finish();
            }
        }

        assertEquals(2, counters.streams());
        assertEquals(1, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.then.end/client/source"
    })
    public void shouldReceiveClientSentDataAndEnd() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

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

        assertEquals(1, counters.streams());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.and.server.sent.data.multiple.frames/client/source"
    })
    public void shouldSendAndReceiveData() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel.write(UTF_8.encode("server data 1"));

                ByteBuffer buf = ByteBuffer.allocate(256);
                buf.limit("client data 1".length());
                channel.read(buf);

                channel.write(UTF_8.encode("server data 2"));
                buf.limit(buf.capacity());
                channel.read(buf);

                buf.flip();

                assertEquals(26, buf.remaining());
                assertEquals("client data 1".concat("client data 2"), UTF_8.decode(buf).toString());

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.close/client/source"
    })
    public void shouldInitiateServerClose() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                channel.shutdownOutput();

                k3po.finish();
            }
        }

    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.close/client/source"
    })
    public void shouldInitiateClientClose() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                ByteBuffer buf = ByteBuffer.allocate(256);
                int len = channel.read(buf);

                assertEquals(-1, len);

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.after.end/client/source"
    })
    public void shouldResetIfDataReceivedAfterEndOfStream() throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                ByteBuffer buf = ByteBuffer.allocate(256);
                channel.read(buf);
                buf.flip();

                assertEquals("client data", UTF_8.decode(buf).toString());

                int len;
                try
                {
                    buf.rewind();
                    len = channel.read(buf);
                }
                catch (IOException ex)
                {
                    len = -1;
                }

                assertEquals(-1, len);

                k3po.finish();
            }
        }
    }
}
