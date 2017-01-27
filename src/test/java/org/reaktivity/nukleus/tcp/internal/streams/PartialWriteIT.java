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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.rule.helper.Helper;
import org.junit.Before;
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
public class PartialWriteIT
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
        .streams("tcp", "target");

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Before
    public void resetWriteHelper()
    {
        TestHelper.reset();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=true, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldSpinWrite() throws Exception
    {
        for (int i=0; i < 10; i++)
        {
            TestHelper.addWriteResult(0);
        }
        shouldReceiveServerSentData();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    @BMUnitConfig(loadDirectory="src/test/resources", debug=true, verbose=false)
    @BMScript(value="PartialWriteIT.btm")
    public void shouldFinishWriteWhenSocketIsWritableAgain() throws Exception
    {
        TestHelper.addWriteResult(5);
        shouldReceiveServerSentData();
    }

    /*
         shouldWriteWhenMoreDataArrivesBeforeSocketWritable
     */

    private void shouldReceiveServerSentData() throws Exception
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

    public static class TestHelper extends Helper
    {
        public static Queue<Integer> writeResults;
        private static int oldLimit;

        protected TestHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public void preWrite(ByteBuffer b)
        {
            if (!callerEquals("org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData",
                    true, true))
            {
                return;
            }
            Integer toWrite = writeResults.peek();
            if (toWrite != null)
            {
                oldLimit = b.limit();
                int originalBytesToWrite = b.limit() - b.position();
                debug(format("preWrite: forcing partial write for buffer %s, writing %d bytes of %d, changing limit to %d",
                        b, toWrite, originalBytesToWrite, b.position() + toWrite));
                b.limit(b.position() + toWrite);
            }
        }

        public void postWrite(ByteBuffer b, int returnValue)
        {
            if (!callerEquals("org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData",
                    true, true))
            {
                return;
            }
            Integer toWrite = writeResults.poll();
            if (toWrite != null)
            {
                debug(format("postWrite: buffer after write is: %s, return value is %d, setting limit back to %d",
                        b, returnValue, oldLimit));
                b.limit(oldLimit);
            }
        }

        private static void addWriteResult(Integer writeResult) {
            writeResults.add(writeResult);
        }

        private static void reset() {
            writeResults = new ArrayDeque(20);
        }

    }

}