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

import java.util.function.LongSupplier;

import org.agrona.concurrent.status.AtomicCounter;

public enum RouteKind
{
    INPUT_NEW
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement)
        {
            // positive, even, non-zero
            return getAndIncrement.getAsLong() << 1L;
        }
    },

    OUTPUT_ESTABLISHED
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement)
        {
            // negative, even, non-zero
            return getAndIncrement.getAsLong() << 1L | 0x8000000000000000L;
        }
    },

    OUTPUT_NEW
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement)
        {
            // positive, odd
            return (getAndIncrement.getAsLong() << 1L) | 1L;
        }
    },

    INPUT_ESTABLISHED
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement)
        {
            // negative, odd
            return (getAndIncrement.getAsLong() << 1L) | 0x8000000000000001L;
        }
    };

    public final long nextRef(
        AtomicCounter counter)
    {
        return nextRef(counter::increment);
    }

    protected abstract long nextRef(
        LongSupplier getAndIncrement);

    public static RouteKind match(
        long referenceId)
    {
        switch ((int)referenceId & 0x01 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000001:
            return OUTPUT_NEW;
        case 0x80000001:
            return INPUT_ESTABLISHED;
        case 0x00000000:
            return INPUT_NEW;
        case 0x80000000:
            return OUTPUT_ESTABLISHED;
        default:
            throw new IllegalArgumentException();
        }
    }
}
