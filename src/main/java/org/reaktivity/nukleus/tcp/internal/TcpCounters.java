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

import org.reaktivity.nukleus.Configuration;

public final class TcpCounters implements AutoCloseable
{
    private final Context context;

    public TcpCounters(Configuration config)
    {
        context = new Context();
        context.readonly(true);
        context.conclude(config);
    }

    public long routes()
    {
        return context.counters().routes().get();
    }

    public long streams()
    {
        return context.counters().streams().get();
    }

    public long overflows()
    {
        return context.counters().overflows().get();
    }

    @Override
    public void close() throws Exception
    {
        context.close();
    }

}
