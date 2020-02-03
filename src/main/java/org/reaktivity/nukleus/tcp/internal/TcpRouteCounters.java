/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class TcpRouteCounters
{
    public final LongSupplier overflows;
    public final LongSupplier partials;

    public final LongSupplier writeops;
    public final LongSupplier writeopsNoSlot;
    public final LongSupplier readops;

    public final LongSupplier opensWritten;
    public final LongConsumer bytesWritten;
    public final LongSupplier closesWritten;
    public final LongSupplier abortsWritten;
    public final LongSupplier resetsWritten;

    public final LongSupplier opensRead;
    public final LongConsumer bytesRead;
    public final LongSupplier closesRead;
    public final LongSupplier abortsRead;
    public final LongSupplier resetsRead;


    TcpRouteCounters(
        long routeId,
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.overflows = supplyCounter.apply("tcp.overflows");
        this.partials = supplyCounter.apply("tcp.partial.writes");

        this.writeopsNoSlot = supplyCounter.apply(String.format("tcp.%d.writeops.noslot", routeId));
        this.writeops = supplyCounter.apply(String.format("tcp.%d.writeops", routeId));
        this.readops = supplyCounter.apply(String.format("tcp.%d.readops", routeId));

        this.opensWritten = supplyCounter.apply(String.format("tcp.%d.opens.written", routeId));
        this.bytesWritten = supplyAccumulator.apply(String.format("tcp.%d.bytes.written", routeId));
        this.closesWritten = supplyCounter.apply(String.format("tcp.%d.closes.written", routeId));
        this.abortsWritten = supplyCounter.apply(String.format("tcp.%d.aborts.written", routeId));
        this.resetsWritten = supplyCounter.apply(String.format("tcp.%d.resets.written", routeId));

        this.opensRead = supplyCounter.apply(String.format("tcp.%d.opens.read", routeId));
        this.bytesRead = supplyAccumulator.apply(String.format("tcp.%d.bytes.read", routeId));
        this.closesRead = supplyCounter.apply(String.format("tcp.%d.closes.read", routeId));
        this.abortsRead = supplyCounter.apply(String.format("tcp.%d.aborts.read", routeId));
        this.resetsRead = supplyCounter.apply(String.format("tcp.%d.resets.read", routeId));
    }
}
