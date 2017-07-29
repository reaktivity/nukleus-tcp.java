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

import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.Configuration.COMMAND_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.DIRECTORY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME;

import java.util.Properties;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.Counters;

public class TcpCountersRule implements TestRule
{
    private final Properties properties;
    private Counters counters;

    public TcpCountersRule()
    {
        this.properties = new Properties();
    }

    public TcpCountersRule directory(String directory)
    {
        properties.setProperty(DIRECTORY_PROPERTY_NAME, directory);
        return this;
    }

    public TcpCountersRule commandBufferCapacity(int commandBufferCapacity)
    {
        properties.setProperty(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(commandBufferCapacity));
        return this;
    }

    public TcpCountersRule responseBufferCapacity(int responseBufferCapacity)
    {
        properties.setProperty(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(responseBufferCapacity));
        return this;
    }

    public TcpCountersRule counterValuesBufferCapacity(int counterValuesBufferCapacity)
    {
        properties.setProperty(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(counterValuesBufferCapacity));
        return this;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                Configuration configuration = new Configuration(properties);
                try(Context context = new Context().name("tcp").readonly(true).conclude(configuration);
                    Counters counters = context.counters())
                {
                    TcpCountersRule.this.counters = counters;
                    assertEquals(0, counters.routes().get());
                    assertEquals(0, counters.streams().get());
                    assertEquals(0, counters.counter("overflows").get());
                    base.evaluate();
                }
            }

        };
    }

    public long routes()
    {
        return counters.routes().get();
    }

    public long streams()
    {
        return counters.streams().get();
    }

    public long overflows()
    {
        return counters.counter("overflows").get();
    }

}
