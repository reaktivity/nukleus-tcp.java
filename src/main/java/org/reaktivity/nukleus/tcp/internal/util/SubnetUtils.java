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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that performs some subnet calculations given a network address and a subnet mask.
 *
 * @see http://www.faqs.org/rfcs/rfc1519.html
 */
public class SubnetUtils
{

    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final String SLASH_FORMAT = IP_ADDRESS + "/(\\d{1,3})";
    private static final Pattern ADDRESS_PATTERN = Pattern.compile(IP_ADDRESS);
    private static final Pattern CIDR_PATTERN = Pattern.compile(SLASH_FORMAT);
    private static final int NBITS = 32;

    private int netmask = 0;
    private int address = 0;
    private int network = 0;
    private int broadcast = 0;

    /**
     * Constructor that takes a CIDR-notation string, e.g. "192.168.0.1/16"
     *
     * @param cidrNotation A CIDR-notation string, e.g. "192.168.0.1/16"
     */
    public SubnetUtils(String cidrNotation)
    {
        calculate(cidrNotation);
    }

    /**
     * Convenience container for subnet summary information.
     */
    public final class SubnetInfo
    {
        private SubnetInfo() {}

        private int network()
        {
            return network;
        }

        private int broadcast()
        {
            return broadcast;
        }

        private int low()
        {
            return network() + 1;
        }

        private int high()
        {
            return broadcast() - 1;
        }

        public boolean isInRange(String address)
        {
            return isInRange(toInteger(address));
        }

        private boolean isInRange(int address)
        {
            return ((address - low()) <= (high() - low()));
        }

    }

    /**
     * Return a {@link SubnetInfo} instance that contains subnet-specific statistics
     */
    public final SubnetInfo getInfo()
    {
        return new SubnetInfo();
    }

    /*
     * Initialize the internal fields from the supplied CIDR mask
     */
    private void calculate(String mask)
    {
        Matcher matcher = CIDR_PATTERN.matcher(mask);

        if (matcher.matches())
        {
            address = matchAddress(matcher);

            /* Create a binary netmask from the number of bits specification /x */
            int cidrPart = rangeCheck(Integer.parseInt(matcher.group(5)), 0, NBITS-1);
            for (int j = 0; j < cidrPart; ++j)
            {
                netmask |= (1 << 31-j);
            }

            /* Calculate base network address */
            network = (address & netmask);

            /* Calculate broadcast address */
            broadcast = network | ~(netmask);
        }
        else
        {
            throw new IllegalArgumentException("Could not parse [" + mask + "]");
        }
    }

    /*
     * Convert a dotted decimal format address to a packed integer format
     */
    private int toInteger(String address)
    {
        Matcher matcher = ADDRESS_PATTERN.matcher(address);
        if (matcher.matches())
        {
            return matchAddress(matcher);
        }
        else
        {
            throw new IllegalArgumentException("Could not parse [" + address + "]");
        }
    }

    /*
     * Convenience method to extract the components of a dotted decimal address and
     * pack into an integer using a regex match
     */
    private int matchAddress(Matcher matcher)
    {
        int addr = 0;
        for (int i = 1; i <= 4; ++i)
        {
            int n = (rangeCheck(Integer.parseInt(matcher.group(i)), 0, 255));
            addr |= ((n & 0xff) << 8*(4-i));
        }
        return addr;
    }

    /*
     * Convenience function to check integer boundaries
     */
    private int rangeCheck(int value, int begin, int end)
    {
        if (value >= begin && value <= end)
        {
            return value;
        }

        throw new IllegalArgumentException("Value out of range: [" + value + "]");
    }
}
