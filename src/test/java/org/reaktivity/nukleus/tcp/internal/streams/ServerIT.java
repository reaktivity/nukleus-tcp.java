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
import java.net.Socket;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
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

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/connection.established/server/target"
    })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        new Socket("127.0.0.1", 0x1f90).close();

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);

            assertEquals("server data", new String(buf, 0, len, UTF_8));
        }

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
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
            assertEquals("server data 1", new String(buf, 0, 13, UTF_8));
            assertEquals("server data 2", new String(buf, 13, offset - 13, UTF_8));
        }
        finally
        {
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90);
             Socket socket2 = new Socket("127.0.0.1", 0x1f90))
        {
            InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);
            assertEquals("server data 1", new String(buf, 0, len, UTF_8));

            in = socket2.getInputStream();
            len = in.read(buf);
            assertEquals("server data 2", new String(buf, 0, len, UTF_8));
        }

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);

            assertEquals("server data", new String(buf, 0, len, UTF_8));
            len = in.read(buf);
            assertEquals(-1, len);
        }

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final OutputStream out = socket.getOutputStream();

            out.write("client data".getBytes());

            k3po.finish();
        }
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final OutputStream out = socket.getOutputStream();

            out.write("client data 1".getBytes());
            k3po.awaitBarrier("FIRST_DATA_FRAME_RECEIVED");
            out.write("client data 2".getBytes());

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

        try (Socket socket1 = new Socket("127.0.0.1", 0x1f90);
             Socket socket2 = new Socket("127.0.0.1", 0x1f90))
        {
            final OutputStream out1 = socket1.getOutputStream();
            final OutputStream out2 = socket2.getOutputStream();
            out1.write("client data 1".getBytes());
            out2.write("client data 2".getBytes());

            k3po.finish();
        }
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final OutputStream out = socket.getOutputStream();

            out.write("client data".getBytes());

            socket.shutdownOutput();

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/echo.data/server/target"
    })
    public void shouldEchoData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();
            final OutputStream out = socket.getOutputStream();

            out.write("client data 1".getBytes());

            byte[] buf1 = new byte[256];
            int len1 = in.read(buf1);

            out.write("client data 2".getBytes());

            byte[] buf2 = new byte[256];
            int len2 = in.read(buf2);

            assertEquals("server data 1", new String(buf1, 0, len1, UTF_8));
            assertEquals("server data 2", new String(buf2, 0, len2, UTF_8));
        }

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);

            assertEquals(-1, len);
        }

        k3po.finish();
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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            socket.shutdownOutput();

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

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);

            assertEquals("server data", new String(buf, 0, len, UTF_8));

            try
            {
                len = in.read(buf);
            }
            catch (IOException ex)
            {
                len = -1;
            }

            assertEquals(-1, len);
        }

        k3po.finish();
    }
}
