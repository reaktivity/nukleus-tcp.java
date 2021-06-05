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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

/**
 * Tests the TCP nukleus when acting as a client.
 */
public class ClientRouteCountersIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/tcp/streams/network/rfc793")
        .addScriptRoot("client", "org/reaktivity/specification/nukleus/tcp/streams/application/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configurationRoot("org/reaktivity/specification/nukleus/tcp/config")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configuration("client.host.json")
    @Specification({
        "${client}/client.and.server.sent.data.multiple.frames/client",
        "${server}/client.and.server.sent.data.multiple.frames/server"
    })
    public void shouldSendAndReceiveData() throws Exception
    {
        k3po.finish();

        assertThat(reaktor.bytesWritten("tcp", Long.toString(0x0000000200000003L)), equalTo(26L));
        assertThat(reaktor.bytesRead("tcp", Long.toString(0x0000000200000003L)), equalTo(26L));
    }
}
