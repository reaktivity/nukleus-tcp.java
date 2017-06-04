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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.generate;
import static org.junit.rules.RuleChain.outerRule;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.tcp.internal.streams.SocketChannelHelper.ProcessDataHelper;
import org.reaktivity.reaktor.test.NukleusRule;

/**
 * Tests the handling of IOException thrown from SocketChannel.read (see issue #9).
 */
@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
public class ServerIOExceptionFromWriteIT
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
    public final TestRule chain = outerRule(SocketChannelHelper.RULE).around(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/server.sent.data.received.reset/server/target"
    })
    @BMRule(name = "processData",
    targetClass = "^java.nio.channels.SocketChannel",
    targetMethod = "write(java.nio.ByteBuffer)",
    condition =
      "callerEquals(\"org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData\", true, true)",
      action = "throw new IOException(\"Simulating an IOException from write\")"
    )
    public void shouldResetWhenImmediateWriteThrowsIOException() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.finish();
        }
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/server.sent.data.received.reset/server/target"
    })
    @BMRules(rules = {
        @BMRule(name = "processData",
        helper = "org.reaktivity.nukleus.tcp.internal.streams.SocketChannelHelper$ProcessDataHelper",
        targetClass = "^java.nio.channels.SocketChannel",
        targetMethod = "write(java.nio.ByteBuffer)",
        condition =
          "callerEquals(\"org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData\", true, true)",
        action = "return doWrite($0, $1);"
        ),
        @BMRule(name = "handleWrite",
        targetClass = "^java.nio.channels.SocketChannel",
        targetMethod = "write(java.nio.ByteBuffer)",
        condition =
          "callerEquals(\"org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.handleWrite\", true, true)",
          action = "throw new IOException(\"Simulating an IOException from write\")"
        )
    })
    public void shouldResetWhenDeferredWriteThrowsIOException() throws Exception
    {
        ProcessDataHelper.fragmentWrites(generate(() -> 0));
        k3po.start();
        k3po.awaitBarrier("ROUTED_SERVER");

        try (SocketChannel channel = SocketChannel.open())
        {
            channel.connect(new InetSocketAddress("127.0.0.1", 0x1f90));

            k3po.finish();
        }
    }

}
