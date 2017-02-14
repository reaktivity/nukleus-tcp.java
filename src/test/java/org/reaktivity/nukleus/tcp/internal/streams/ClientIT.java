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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

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

    private final TcpCountersRule tcpCounters = new TcpCountersRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(tcpCounters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/connection.established/client/source"
    })
    public void shouldEstablishConnection() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final OutputStream out = socket.getOutputStream();

                out.write("server data".getBytes());

                k3po.finish();
            }
        }
        assertEquals(1, tcpCounters.counters().streams().get());
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.multiple.frames/client/source"
    })
    public void shouldReceiveServerSentDataMultipleFrames() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final OutputStream out = socket.getOutputStream();

                out.write("server data 1".getBytes());
                k3po.awaitBarrier("FIRST_DATA_FRAME_RECEIVED");
                out.write("server data 2".getBytes());

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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket1 = server.accept();
                 Socket socket2 = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final OutputStream out1 = socket1.getOutputStream();
                out1.write("server data 1".getBytes());

                final OutputStream out2 = socket2.getOutputStream();
                out2.write("server data 2".getBytes());

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/server.sent.data.then.end/client/source"
    })
    public void shouldReceiveServerSentDataAndEnd() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final OutputStream out = socket.getOutputStream();

                out.write("server data".getBytes());

                socket.shutdownOutput();

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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();

                byte[] buf = new byte[256];
                int len = in.read(buf);

                assertEquals("client data", new String(buf, 0, len, UTF_8));

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.multiple.frames/client/source"
    })
    public void shouldReceiveClientSentDataMultipleFrames() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();

                byte[] buf = new byte[256];
                int offset = 0;

                int read = 0;
                do
                {
                    read = in.read(buf, offset, buf.length - offset);
                    if (read == -1)
                    {
                        break;
                    }
                    offset += read;
                } while (offset < 26);
                assertEquals("client data 1client data 2", new String(buf, 0, offset, UTF_8));
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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept();
                 Socket socket2 = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                InputStream in = socket.getInputStream();
                byte[] buf = new byte[256];
                int len = in.read(buf);
                assertEquals("client data 1", new String(buf, 0, len, UTF_8));

                in = socket2.getInputStream();
                len = in.read(buf);
                assertEquals("client data 2", new String(buf, 0, len, UTF_8));

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.then.end/client/source"
    })
    public void shouldReceiveClientSentDataAndEnd() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();

                byte[] buf = new byte[256];
                int len = in.read(buf);

                assertEquals("client data", new String(buf, 0, len, UTF_8));
                len = in.read();
                assertEquals(-1, len);

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/echo.data/client/source"
    })
    public void shouldEchoData() throws Exception
    {
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();
                final OutputStream out = socket.getOutputStream();

                out.write("server data 1".getBytes());

                byte[] buf = new byte[26];
                int bytes = in.read(buf);

                out.write("server data 2".getBytes());

                bytes += in.read(buf, bytes, buf.length - bytes);

                assertEquals(26, bytes);
                assertEquals("client data 1", new String(buf, 0, 13, UTF_8));
                assertEquals("client data 2", new String(buf, 13, 13, UTF_8));

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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                socket.shutdownOutput();
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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();

                byte[] buf = new byte[256];
                int len = in.read(buf);

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
        try (ServerSocket server = new ServerSocket())
        {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));
            server.setSoTimeout((int) SECONDS.toMillis(5));

            k3po.start();
            k3po.awaitBarrier("ROUTED_OUTPUT");

            try (Socket socket = server.accept())
            {
                k3po.notifyBarrier("ROUTED_INPUT");

                final InputStream in = socket.getInputStream();

                byte[] buf = new byte[256];
                int len = in.read(buf);

                assertEquals("client data", new String(buf, 0, len, UTF_8));

                try
                {
                    len = in.read(buf);
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
