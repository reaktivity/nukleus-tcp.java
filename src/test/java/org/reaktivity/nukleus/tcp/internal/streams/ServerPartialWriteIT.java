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
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory.WRITE_SPIN_COUNT;

import java.io.InputStream;
import java.net.Socket;
import java.util.List;

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
import org.reaktivity.reaktor.test.NukleusRule;

/**
 * This test verifies the handling of incomplete writes, when attempts to write data to a socket channel
 * fail to write out all of the data. In real life this would happen when a client is reading data at a lower
 * speed than it is being written by the server. For testing purposes this test simulates the condition
 * by rewriting the bytecode of the SocketChannelImpl.write method to make that method exhibit the behavior of
 * incomplete writes.
 */
@RunWith(org.jboss.byteman.contrib.bmunit.BMUnitRunner.class)
public class ServerPartialWriteIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("streamsInvalid", "org/reaktivity/specification/nukleus/tcp/streams.invalid")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/tcp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("tcp")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .streams("tcp", "target");

    @Rule
    public final TestRule chain = outerRule(PartialWriteHelper.RULE).around(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldSpinWrite() throws Exception
    {
        for (int i=0; i < WRITE_SPIN_COUNT - 1; i++)
        {
            PartialWriteHelper.addWriteResult(0);
        }
        shouldReceiveServerSentData("server data");
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldFinishWriteWhenSocketIsWritableAgain() throws Exception
    {
        PartialWriteHelper.addWriteResult(5);
        shouldReceiveServerSentData("server data");
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldHandleMultiplePartialWrites() throws Exception
    {
        PartialWriteHelper.addWriteResult(2);
        PartialWriteHelper.addWriteResult(3);
        PartialWriteHelper.addWriteResult(1);
        shouldReceiveServerSentData("server data");
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.multiple.frames/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldWriteWhenMoreDataArrivesWhileAwaitingSocketWritable() throws Exception
    {
        PartialWriteHelper.addWriteResult(5);
        PartialWriteHelper.addWriteResult(8);
        PartialWriteHelper.addWriteResult(5);
        PartialWriteHelper.addWriteResult(5);
        shouldReceiveServerSentData("server data 1server data 2");

        // Verify we forced the desired condition: check handleWrite got called
        // AFTER processData was called for the second frame
        List<String> callers = PartialWriteHelper.callers();
        String error = "Test failed to force desired condition, caller sequence was: " + callers;
        assertTrue(error, callers.lastIndexOf("handleWrite") >  callers.lastIndexOf("processData"));
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data.and.close/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldHandleEndOfStreamWithPendingWrite() throws Exception
    {
        PartialWriteHelper.addWriteResult(2);
        PartialWriteHelper.addWriteResult(2);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        shouldReceiveServerSentData("server data", true);
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streamsInvalid}/server.sent.data.close.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=false, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldResetIfDataReceivedAfterEndOfStreamWithPendingWrite() throws Exception
    {
        PartialWriteHelper.addWriteResult(2);
        PartialWriteHelper.addWriteResult(2);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        PartialWriteHelper.addWriteResult(1);
        shouldReceiveServerSentData("server data", true);
    }

    private void shouldReceiveServerSentData(String expectedData) throws Exception
    {
        shouldReceiveServerSentData(expectedData, false);
    }

    private void shouldReceiveServerSentData(String expectedData, boolean expectStreamClosed) throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[expectedData.length() + 10];
            int offset = 0;

            int read = 0;
            boolean closed = false;
            do
            {
                read = in.read(buf, offset, buf.length - offset);
                if (read == -1)
                {
                    closed = true;
                    break;
                }
                offset += read;
            } while (offset < expectedData.length());
            assertEquals(expectedData, new String(buf, 0, offset, UTF_8));

            if (expectStreamClosed)
            {
                if (!closed)
                {
                    closed = (in.read() == -1);
                }
                assertTrue("Stream was not closed", closed);
            }
        }

        k3po.finish();
    }

}