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
import static java.util.concurrent.TimeUnit.SECONDS;
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
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.specification.nukleus.NukleusRule;

public class ClientLimitsIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/tcp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .controller(TcpController.class::isAssignableFrom)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        // Initial window size for output to network:
        .configure(ReaktorConfiguration.BUFFER_SLOT_CAPACITY_PROPERTY, 16)
        // Overall buffer pool size same as slot size so maximum concurrent streams with partial writes = 1
        .configure(ReaktorConfiguration.BUFFER_POOL_CAPACITY_PROPERTY, 64);

    private final NukleusRule file = new NukleusRule()
            .directory("target/nukleus-itests")
            .streams("tcp", "source#partition")
            .streams("source", "tcp#source");

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(file).around(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${streams}/client.sent.data.received.reset/client/source"
    })
    public void shouldResetWhenWindowExceeded() throws Exception
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

                int len;
                try
                {
                    ByteBuffer buf = ByteBuffer.allocate(256);
                    len = channel.read(buf);
                }
                catch (IOException ex)
                {
                    len = -1;
                }

                //assertEquals(-1, len);

                k3po.finish();
            }
        }
    }
}
