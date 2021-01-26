/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.TcpCountersRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ServerIOExceptionFromReadIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/tcp/streams/application/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("tcp"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .affinityMask("app#0", EXTERNAL_AFFINITY_MASK)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .clean();

    private final TcpCountersRule counters = new TcpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.received.reset.and.abort/server"
    })
    public void shouldReportIOExceptionFromReadAsAbortAndReset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.awaitBarrier("CONNECTED");

            channel.setOption(StandardSocketOptions.SO_LINGER, 0);
            channel.close();

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/server.received.abort.sent.end/server"
    })
    public void shouldNotResetWhenProcessingEndAfterIOExceptionFromRead() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.awaitBarrier("CONNECTED");

            channel.setOption(StandardSocketOptions.SO_LINGER, 0);
            channel.close();

            k3po.finish();
        }
    }

}
