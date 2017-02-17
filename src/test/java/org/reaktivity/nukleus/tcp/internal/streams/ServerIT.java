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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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

public class ServerIT
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
        .streams("tcp", "target#partition");

    private final TcpCountersRule counters = new TcpCountersRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/connection.established/server/target"
    })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.finish();
        }

        assertEquals(1, counters.streams());
        assertEquals(0, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            channel.read(buf);
            buf.flip();

            assertEquals("server data", UTF_8.decode(buf).toString());

            k3po.finish();
        }

        assertEquals(1, counters.streams());
        assertEquals(0, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.multiple.frames/server/target"
    })
    public void shouldReceiveServerSentDataMultipleFrames() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            do
            {
                int len = channel.read(buf);
                if (len == -1)
                {
                    break;
                }
            } while (buf.position() < 26);
            buf.flip();

            assertEquals("server data 1".concat("server data 2"), UTF_8.decode(buf).toString());

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.multiple.streams/server/target"
    })
    public void shouldReceiveServerSentDataMultipleStreams() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel1 = SocketChannel.open();
             SocketChannel channel2 = SocketChannel.open())
        {
            channel1.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel2.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            channel1.read(buf);
            buf.flip();
            assertEquals("server data 1", UTF_8.decode(buf).toString());

            buf.rewind();
            channel2.read(buf);
            buf.flip();
            assertEquals("server data 2", UTF_8.decode(buf).toString());

            k3po.finish();
        }

        assertEquals(2, counters.streams());
        assertEquals(0, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.then.end/server/target"
    })
    public void shouldReceiveServerSentDataAndEnd() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            channel.read(buf);
            buf.flip();

            assertEquals("server data", UTF_8.decode(buf).toString());

            buf.rewind();
            int len = channel.read(buf);

            assertEquals(-1, len);

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.sent.data/server/target"
    })
    public void shouldReceiveClientSentData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel.write(UTF_8.encode("client data"));

            k3po.finish();
        }

        assertEquals(1, counters.streams());
        assertEquals(0, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.sent.data.multiple.frames/server/target"
    })
    public void shouldReceiveClientSentDataMultipleFrames() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel.write(UTF_8.encode("client data 1"));

            k3po.awaitBarrier("FIRST_DATA_FRAME_RECEIVED");

            channel.write(UTF_8.encode("client data 2"));

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.sent.data.multiple.streams/server/target"
    })
    public void shouldReceiveClientSentDataMultipleStreams() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel1 = SocketChannel.open();
             SocketChannel channel2 = SocketChannel.open())
        {
            channel1.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel2.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            channel1.write(UTF_8.encode("client data 1"));
            channel2.write(UTF_8.encode("client data 2"));

            k3po.finish();
        }

        assertEquals(2, counters.streams());
        assertEquals(0, counters.routes());
        assertEquals(0, counters.overflows());
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.sent.data.then.end/server/target"
    })
    public void shouldReceiveClientSentDataAndEnd() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel.write(UTF_8.encode("client data"));
            channel.shutdownOutput();

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.and.server.sent.data.multiple.frames/server/target"
    })
    public void shouldSendAndReceiveData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel.write(UTF_8.encode("client data 1"));

            ByteBuffer buf1 = ByteBuffer.allocate(256);
            channel.read(buf1);
            buf1.flip();

            channel.write(UTF_8.encode("client data 2"));

            ByteBuffer buf2 = ByteBuffer.allocate(256);
            channel.read(buf2);
            buf2.flip();

            assertEquals("server data 1", UTF_8.decode(buf1).toString());
            assertEquals("server data 2", UTF_8.decode(buf2).toString());

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.close/server/target"
    })
    public void shouldInitiateServerClose() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            int len = channel.read(buf);
            buf.flip();

            assertEquals(-1, len);

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.close/server/target"
    })
    public void shouldInitiateClientClose() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));
            channel.shutdownOutput();

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.after.end/server/target"
    })
    public void shouldResetIfDataReceivedAfterEndOfStream() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            ByteBuffer buf = ByteBuffer.allocate(256);
            channel.read(buf);
            buf.flip();

            assertEquals("server data", UTF_8.decode(buf).toString());

            int len = 0;
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
