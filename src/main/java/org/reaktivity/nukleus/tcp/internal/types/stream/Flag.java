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
package org.reaktivity.nukleus.tcp.internal.types.stream;

public enum Flag
{
    FIN,
    RST;

    public int flag()
    {
        return mask;
    }

    public boolean check(
        int flags)
    {
        return (flags & mask) != 0;
    }

    public int set(
        int flags)
    {
        return flags | mask;
    }

    public int clear(
        int flags)
    {
        return flags & ~mask;
    }

    private final int mask;

    Flag()
    {
        this.mask = 1 << ordinal();
    }
}
