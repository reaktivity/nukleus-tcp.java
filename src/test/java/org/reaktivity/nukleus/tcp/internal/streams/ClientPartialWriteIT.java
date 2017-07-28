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
import static java.util.stream.IntStream.concat;
import static java.util.stream.IntStream.generate;
import static java.util.stream.IntStream.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.tcp.internal.streams.SocketChannelHelper.ALL;
import static org.reaktivity.nukleus.tcp.internal.stream.WriteStream.WRITE_SPIN_COUNT;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.streams.SocketChannelHelper.HandleWriteHelper;
import org.reaktivity.nukleus.tcp.internal.streams.SocketChannelHelper.ProcessDataHelper;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.specification.nukleus.NukleusRule;

@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
@BMUnitConfig(loadDirectory="src/test/resources")
@BMScript(value="SocketChannelHelper.btm")
public class ClientPartialWriteIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/tcp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    private final NukleusRule file = new NukleusRule()
            .directory("target/nukleus-itests")
            .streams("tcp", "source#partition")
            .streams("source", "tcp#source");

    @Rule
    public final TestRule chain = outerRule(SocketChannelHelper.RULE).around(file).around(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data/client/source"
    })
    public void shouldSpinWrite() throws Exception
    {
        ProcessDataHelper.fragmentWrites(generate(() -> 0).limit(WRITE_SPIN_COUNT - 1));
        HandleWriteHelper.fragmentWrites(generate(() -> 0));
        shouldReceiveClientSentData("client data");
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data/client/source"
    })
    public void shouldFinishWriteWhenSocketIsWritableAgain() throws Exception
    {
        ProcessDataHelper.fragmentWrites(IntStream.of(5));
        shouldReceiveClientSentData("client data");
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data/client/source"
    })
    public void shouldHandleMultiplePartialWrites() throws Exception
    {
        ProcessDataHelper.fragmentWrites(IntStream.of(2));
        HandleWriteHelper.fragmentWrites(IntStream.of(3, 1));
        shouldReceiveClientSentData("client data");
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data.multiple.frames/client/source"
    })
    public void shouldWriteWhenMoreDataArrivesWhileAwaitingSocketWritable() throws Exception
    {
        // processData will be called for each of the two data frames. Make the first
        // do a partial write, then write nothing until handleWrite is called after the
        // second processData call, when we write everything.
        AtomicBoolean finishWrite = new AtomicBoolean(false);

        ProcessDataHelper.fragmentWrites(concat(of(5), generate(() -> finishWrite.getAndSet(true) ? 0 : 0)));
        HandleWriteHelper.fragmentWrites(generate(() -> finishWrite.get() ? ALL : 0));

        shouldReceiveClientSentData("client data 1client data 2");
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data.then.end/client/source"
    })
    public void shouldHandleEndOfStreamWithPendingWrite() throws Exception
    {
        AtomicBoolean endWritten = new AtomicBoolean(false);
        ProcessDataHelper.fragmentWrites(concat(of(5), generate(() -> 0)));
        HandleWriteHelper.fragmentWrites(generate(() -> endWritten.get() ? ALL : 0));

        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("CONNECTED_CLIENT");
                k3po.awaitBarrier("END_WRITTEN");
                endWritten.set(true);

                ByteBuffer buf = ByteBuffer.allocate("client data".length() + 10);
                boolean closed = false;
                do
                {
                    int len = channel.read(buf);
                    if (len == -1)
                    {
                        closed = true;
                        break;
                    }
                } while (buf.position() < "client data".length());
                buf.flip();

                assertEquals("client data", UTF_8.decode(buf).toString());

                if (!closed)
                {
                    buf.rewind();
                    closed = (channel.read(buf) == -1);
                }

                assertTrue("Stream was not closed", closed);

                k3po.finish();
            }
        }
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data.after.end/client/source"
    })
    public void shouldResetIfDataReceivedAfterEndOfStreamWithPendingWrite() throws Exception
    {
        ProcessDataHelper.fragmentWrites(IntStream.of(6));
        AtomicBoolean resetReceived = new AtomicBoolean(false);
        HandleWriteHelper.fragmentWrites(generate(() -> resetReceived.get() ? ALL : 0));

        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("CONNECTED_CLIENT");

                k3po.awaitBarrier("RESET_CLIENT");
                resetReceived.set(true);

                ByteBuffer buf = ByteBuffer.allocate("client data".length() + 10);
                boolean closed = false;
                do
                {
                    int len = channel.read(buf);
                    if (len == -1)
                    {
                        closed = true;
                        break;
                    }
                } while (buf.position() < "client data".length());
                buf.flip();

                assertEquals("client data", UTF_8.decode(buf).toString());

                if (!closed)
                {
                    buf.rewind();
                    closed = (channel.read(buf) == -1);
                }

                assertTrue("Stream was not closed", closed);

                k3po.finish();
            }
        }
    }

    private void shouldReceiveClientSentData(String expectedData) throws Exception
    {
        shouldReceiveClientSentData(expectedData, false);
    }

    private void shouldReceiveClientSentData(String expectedData, boolean expectStreamClosed) throws Exception
    {
        try (ServerSocketChannel server = ServerSocketChannel.open())
        {
            server.setOption(SO_REUSEADDR, true);
            server.bind(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.start();
            k3po.awaitBarrier("ROUTED_CLIENT");

            try (SocketChannel channel = server.accept())
            {
                k3po.notifyBarrier("CONNECTED_CLIENT");

                ByteBuffer buf = ByteBuffer.allocate(expectedData.length() + 10);
                boolean closed = false;
                do
                {
                    int len = channel.read(buf);
                    if (len == -1)
                    {
                        closed = true;
                        break;
                    }
                } while (buf.position() < expectedData.length());
                buf.flip();

                assertEquals(expectedData, UTF_8.decode(buf).toString());

                if (expectStreamClosed)
                {
                    if (!closed)
                    {
                        buf.rewind();
                        closed = (channel.read(buf) == -1);
                    }

                    assertTrue("Stream was not closed", closed);
                }

                k3po.finish();
            }
        }
    }

}
