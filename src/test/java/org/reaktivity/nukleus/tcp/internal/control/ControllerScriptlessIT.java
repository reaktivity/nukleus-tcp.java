/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerScriptlessIT
{
    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleus("tcp"::equals)
        .controller("tcp"::equals);

    @Rule
    public final TestRule chain = outerRule(timeout).around(reaktor);

    @Test(expected = IllegalArgumentException.class)
    public void shouldRefuseRouteServerPortZero() throws Exception
    {
        long targetRef = new Random().nextLong();

        reaktor.controller(TcpController.class)
               .routeServer("0.0.0.0", 0, "target", targetRef)
               .get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRefuseRouteServerNegativePort() throws Exception
    {
        long targetRef = new Random().nextLong();

        reaktor.controller(TcpController.class)
               .routeServer("0.0.0.0", -1L, "target", targetRef)
               .get();
    }
}
