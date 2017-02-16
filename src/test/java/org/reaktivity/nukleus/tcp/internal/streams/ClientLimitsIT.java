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
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.MAXIMUM_STREAMS_WITH_PENDING_WRITES;
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.WINDOW_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

@Ignore
public class ClientLimitsIT
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
        .streams("tcp", "source");

    private final TestRule properties = new SystemPropertiesRule()
            .setProperty(MAXIMUM_STREAMS_WITH_PENDING_WRITES.propertyName(), "1")
            .setProperty(WINDOW_SIZE.propertyName(), "15");

    @Rule
    public final TestRule chain = outerRule(properties).around(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.received.reset/client/source"
    })
    @Ignore // TODO: failing on Travis
    public void shouldResetWhenWindowExceeded() throws Exception
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
                socket.setSoTimeout((int) SECONDS.toMillis(4));
                k3po.notifyBarrier("ROUTED_INPUT");
                final InputStream in = socket.getInputStream();
                int len;
                try
                {
                    len = in.read();
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
