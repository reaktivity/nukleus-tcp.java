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

import org.agrona.concurrent.status.AtomicCounter;

public enum RouteKind
{
    SERVER_INITIAL
    {
        @Override
        public int kind()
        {
            return 0x21;
        }

        @Override
        public final long nextRef(
            AtomicCounter counter)
        {
            // positive, even, non-zero
            counter.increment();
            return counter.get() << 1L;
        }
    },

    SERVER_REPLY
    {
        @Override
        public int kind()
        {
            return 0x22;
        }

        @Override
        public final long nextRef(
            AtomicCounter counter)
        {
            // negative, even, non-zero
            counter.increment();
            return counter.get() << 1L | 0x8000000000000000L;
        }
    },

    CLIENT_INITIAL
    {
        @Override
        public int kind()
        {
            return 0x11;
        }

        @Override
        public final long nextRef(
            AtomicCounter counter)
        {
            // positive, odd
            return (counter.increment() << 1L) | 1L;
        }
    },

    CLIENT_REPLY
    {
        @Override
        public int kind()
        {
            return 0x12;
        }

        @Override
        public final long nextRef(
            AtomicCounter counter)
        {
            // negative, odd
            return (counter.increment() << 1L) | 0x8000000000000001L;
        }
    };

    public abstract long nextRef(
        AtomicCounter counter);

    public abstract int kind();

    public static RouteKind of(
        int kind)
    {
        switch (kind)
        {
        case 0x11:
            return CLIENT_INITIAL;
        case 0x12:
            return CLIENT_REPLY;
        case 0x21:
            return SERVER_INITIAL;
        case 0x22:
            return SERVER_REPLY;
        default:
            throw new IllegalArgumentException("Unexpected kind: " + kind);
        }
    }

    public static RouteKind match(
        long referenceId)
    {
        switch ((int)referenceId & 0x01 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000001:
            return CLIENT_INITIAL;
        case 0x80000001:
            return CLIENT_REPLY;
        case 0x00000000:
            return SERVER_INITIAL;
        case 0x80000000:
            return SERVER_REPLY;
        default:
            throw new IllegalArgumentException();
        }
    }
}
