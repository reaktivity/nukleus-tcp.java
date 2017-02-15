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

import static org.agrona.IoUtil.unmap;

import org.agrona.concurrent.status.CountersManager;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.tcp.internal.layouts.ControlLayout;

public final class TcpCounters implements AutoCloseable
{
    private final ControlLayout controlRO;
    private final Counters counters;

    public TcpCounters(Configuration config)
    {
        ControlLayout.Builder controlRW = new ControlLayout.Builder();
        controlRO = controlRW.controlPath(config.directory().resolve("tcp/control"))
            .commandBufferCapacity(config.commandBufferCapacity())
            .responseBufferCapacity(config.responseBufferCapacity())
            .counterLabelsBufferCapacity(config.counterLabelsBufferCapacity())
            .counterValuesBufferCapacity(config.counterValuesBufferCapacity())
            .readonly(true)
            .build();
        unmap(controlRO.commandBuffer().byteBuffer());
        unmap(controlRO.responseBuffer().byteBuffer());
        counters = new Counters(new CountersManager(
                controlRO.counterLabelsBuffer(),
                controlRO.counterValuesBuffer()));
    }

    Counters counters()
    {
        return counters;
    }

    @Override
    public void close() throws Exception
    {
        counters.close();
        unmap(controlRO.counterLabelsBuffer().byteBuffer());
        unmap(controlRO.counterValuesBuffer().byteBuffer());

    }

}
