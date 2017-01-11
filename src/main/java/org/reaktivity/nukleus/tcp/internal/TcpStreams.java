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
package org.reaktivity.nukleus.tcp.internal;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.tcp.internal.layouts.StreamsLayout;

public final class TcpStreams
{
    private final StreamsLayout layout;
    private final RingBuffer streams;
    private final RingBuffer throttle;

    TcpStreams(
        Context context,
        String source,
        String target)
    {
        this.layout = new StreamsLayout.Builder()
                .streamsCapacity(context.streamsBufferCapacity())
                .throttleCapacity(context.throttleBufferCapacity())
                .path(context.routeStreamsPath().apply(source, target))
                .readonly(true)
                .build();

        this.streams = this.layout.streamsBuffer();
        this.throttle = this.layout.throttleBuffer();
    }

    public void close()
    {
        layout.close();
    }

    public int readStreams(
        MessageHandler handler)
    {
        return streams.read(handler);
    }

    public boolean writeThrottle(
        int msgTypeId,
        DirectBuffer srcBuffer,
        int srcIndex,
        int length)
    {
        return throttle.write(msgTypeId, srcBuffer, srcIndex, length);
    }
}
