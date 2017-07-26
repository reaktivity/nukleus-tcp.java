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
package org.reaktivity.nukleus.tcp.internal.reader.stream;

import static java.nio.ByteOrder.nativeOrder;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.LongFunction;
import java.util.function.ToIntFunction;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.reader.Target;
import org.reaktivity.nukleus.tcp.internal.router.Correlation;
import org.reaktivity.nukleus.tcp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

public final class ReaderStreamFactory
{
    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    final int bufferSize;
    final LongFunction<Correlation> resolveCorrelation;
    final ByteBuffer readBuffer;
    final AtomicBuffer atomicBuffer;

    public ReaderStreamFactory(
        int maxMessageLength,
        LongFunction<Correlation> resolveCorrelation)
    {
        this.bufferSize = maxMessageLength - DataFW.FIELD_OFFSET_PAYLOAD;
        this.resolveCorrelation = resolveCorrelation;
        this.readBuffer = ByteBuffer.allocateDirect(bufferSize).order(nativeOrder());
        this.atomicBuffer = new UnsafeBuffer(readBuffer);
    }

    public ToIntFunction<PollerKey> newStream(
        Target target,
        long targetId,
        PollerKey key,
        SocketChannel channel,
        long correlationId)
    {
        final ReaderStream stream = new ReaderStream(this, target, targetId, key, channel, correlationId);

        target.addThrottle(targetId, stream::handleThrottle);

        return stream::handleStream;
    }
}
