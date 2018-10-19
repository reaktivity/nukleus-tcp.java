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
package org.reaktivity.nukleus.tcp.internal;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class TcpRouteCounters
{
    public final LongSupplier overflows;
    public final LongSupplier connectionsOpened;
    public final LongSupplier connectionsClosed;
    public final LongSupplier framesWritten;
    public final LongConsumer bytesWritten;
    public final LongSupplier framesRead;
    public final LongConsumer bytesRead;
    public final LongSupplier connectFailed;

    TcpRouteCounters(
        long routeId,
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.overflows = supplyCounter.apply("overflows");
        this.connectionsOpened = supplyCounter.apply(String.format("%d.connections.opened", routeId));
        this.connectionsClosed = supplyCounter.apply(String.format("%d.connections.closed", routeId));
        this.framesWritten = supplyCounter.apply(String.format("%d.frames.written", routeId));
        this.bytesWritten = supplyAccumulator.apply(String.format("%d.bytes.written", routeId));
        this.framesRead = supplyCounter.apply(String.format("%d.frames.read", routeId));
        this.bytesRead = supplyAccumulator.apply(String.format("%d.bytes.read", routeId));
        this.connectFailed = supplyCounter.apply(String.format("%d.connect.failed", routeId));
    }
}
