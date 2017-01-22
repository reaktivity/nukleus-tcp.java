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
package org.reaktivity.nukleus.tcp.internal.router;

import static org.junit.Assert.assertSame;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class RouteKindTest
{
    private static final long MAX_COUNTER = 0x07ffffffffffffffL;

    @Test
    public void shouldMatchKindWithGeneratedRef()
    {
        long initialValue = new Random().nextLong() & MAX_COUNTER;
        AtomicLong counter = new AtomicLong(initialValue);

        for (RouteKind kind : RouteKind.values())
        {
            long ref = kind.nextRef(counter::getAndIncrement, counter::get);

            String message = String.format("%s ref does not match", kind);
            assertSame(message, kind, RouteKind.match(ref));
        }
    }
}
