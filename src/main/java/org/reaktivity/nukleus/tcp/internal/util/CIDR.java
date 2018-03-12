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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.reaktivity.nukleus.tcp.internal.util;

import static java.lang.Integer.parseInt;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CIDR
{
    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final String SLASH_FORMAT = IP_ADDRESS + "/(\\d{1,3})";
    private static final Pattern ADDRESS_PATTERN = Pattern.compile(IP_ADDRESS);
    private static final Pattern CIDR_PATTERN = Pattern.compile(SLASH_FORMAT);

    private final long low;
    private final long high;


    public CIDR(String cidrNotation)
    {
        final Matcher matcher = CIDR_PATTERN.matcher(cidrNotation);

        if (matcher.matches())
        {
            final long address = matchAddress(matcher);

            final int cidrPart = Integer.parseInt(matcher.group(5));
            int netmask = 0;
            for (int j = 0; j < cidrPart; ++j)
            {
                netmask |= (1 << 31-j);
            }

            final long network = (address & netmask);

            final long broadcast = network | ~(netmask);
            low = network;
            high = broadcast  == -1 ? Long.MAX_VALUE : broadcast;
        }
        else
        {
            throw new IllegalArgumentException("Could not parse [" + cidrNotation + "]");
        }
    }

    public boolean isInRange(String address)
    {
        final long addr = toLong(address);
        return high >= addr && addr >= low;
    }

    private long toLong(String address)
    {
        final Matcher matcher = ADDRESS_PATTERN.matcher(address);
        if (matcher.matches())
        {
            return matchAddress(matcher);
        }
        else
        {
            throw new IllegalArgumentException("Could not parse [" + address + "]");
        }
    }

    private long matchAddress(Matcher matcher)
    {
        long addr = 0;
        for (int i = 1; i <= 4; ++i)
        {
            int n = parseInt(matcher.group(i));
            addr = addr << 8;
            addr |= n & 0xff;
        }
        return addr;
    }
}
