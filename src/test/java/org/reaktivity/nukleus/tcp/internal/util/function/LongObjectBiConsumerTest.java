/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.util.function;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

public final class LongObjectBiConsumerTest
{
    @Rule
    public final JUnitRuleMockery mockery = new JUnitRuleMockery();

    @Test
    public void shouldAcceptPrimitiveLongObject()
    {
        @SuppressWarnings("unchecked")
        final LongObjectBiConsumer<Object> consumer = mockery.mock(LongObjectBiConsumer.class);

        mockery.checking(new Expectations()
        {
            {
                allowing(consumer).accept(with(0L), with(any(Object.class)));
            }
        });

        consumer.accept(0L, new Object());
    }

    @Test
    public void shouldAcceptBoxedLongObject()
    {
        @SuppressWarnings("unchecked")
        final LongObjectBiConsumer<Object> consumer = mockery.mock(LongObjectBiConsumer.class);

        mockery.checking(new Expectations()
        {
            {
                allowing(consumer).accept(with(0L), with(any(Object.class)));
            }
        });

        final LongObjectBiConsumer<Object> defaults = consumer::accept;
        defaults.accept(Long.valueOf(0L), new Object());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldComposeLongObjectBiConsumer()
    {
        final LongObjectBiConsumer<Object> primary = mockery.mock(LongObjectBiConsumer.class, "primary");
        final LongObjectBiConsumer<Object> secondary = mockery.mock(LongObjectBiConsumer.class, "secondary");

        mockery.checking(new Expectations()
        {
            {
                allowing(primary).accept(with(0L), with(any(Object.class)));
                allowing(secondary).accept(with(0L), with(any(Object.class)));
            }
        });

        final LongObjectBiConsumer<Object> defaults = primary::accept;
        defaults.andThen(secondary).accept(0L, new Object());
    }
}
