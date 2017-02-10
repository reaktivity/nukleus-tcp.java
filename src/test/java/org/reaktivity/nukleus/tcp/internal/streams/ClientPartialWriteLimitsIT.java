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
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.MAXIMUM_STREAMS_WITH_PENDING_WRITES;
import static org.reaktivity.nukleus.tcp.internal.InternalSystemProperty.WINDOW_SIZE;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

/**
 * Tests the handling of capacity exceeded conditions in the context of incomplete writes
 */
@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
public class ClientPartialWriteLimitsIT
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
    public final TestRule chain = outerRule(PartialWriteHelper.RULE).around(properties)
                                  .around(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/client.sent.data.multiple.streams/client/source"
    })
    @ScriptProperty("secondStreamWriteReplyFrameType [0x40 0x00 0x00 0x01]")
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldResetStreamsExceedingPartialWriteStreamsLimit() throws Exception
    {
        PartialWriteHelper.addWriteResult(1); // avoid spin write for first stream write
        AtomicBoolean resetReceived = new AtomicBoolean(false);
        PartialWriteHelper.setWriteResultSupplier(() -> resetReceived.get() ? null : 0);

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

                k3po.awaitBarrier("SECOND_STREAM_WRITE_REPLY_RECEIVED");
                resetReceived.set(true);

                InputStream in = socket.getInputStream();
                byte[] buf = new byte[256];
                int offset = 0;
                while (offset < 13)
                {
                    int len = in.read(buf, offset, buf.length - offset);
                    assert (len != -1);
                    offset += len;
                }
                assertEquals("client data 1", new String(buf, 0, offset, UTF_8));

                in = socket2.getInputStream();
                offset = 0;
                int len = 0;
                while (offset < 13)
                {
                    len = in.read(buf, offset, buf.length - offset);
                    if (len == -1)
                    {
                        break;
                    }
                    offset += len;
                }
                assertEquals(-1, len);

                k3po.finish();
            }
        }
    }
}
