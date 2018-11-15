/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public final class SubnetUtilTest
{

    @Test
    public void shouldHonorHostRoute() throws Exception
    {
        CIDR cidr = new CIDR("127.127.127.127/32");
        assertTrue(cidr.isInRange("127.127.127.127"));
        assertFalse(cidr.isInRange("127.127.127.128"));
        assertFalse(cidr.isInRange("127.127.127.126"));
        assertFalse(cidr.isInRange("255.255.255.127"));
        assertFalse(cidr.isInRange("0.0.0.127"));
    }

    @Test
    public void shouldHonorArbitrary() throws Exception
    {
        CIDR cidr = new CIDR("127.127.255.0/20");
        assertTrue(cidr.isInRange("127.127.240.0"));
        assertFalse(cidr.isInRange("127.127.239.255"));
        assertTrue(cidr.isInRange("127.127.255.255"));
        assertFalse(cidr.isInRange("127.128.0.0"));
    }

    @Test
    public void shouldHonorClassC() throws Exception
    {
        CIDR cidr = new CIDR("127.127.127.0/24");
        assertTrue(cidr.isInRange("127.127.127.255"));
        assertTrue(cidr.isInRange("127.127.127.0"));
        assertFalse(cidr.isInRange("127.127.126.255"));
        assertFalse(cidr.isInRange("127.127.128.0"));
    }

    @Test
    public void shouldHonorClassB() throws Exception
    {
        CIDR cidr = new CIDR("127.127.127.127/16");
        assertTrue(cidr.isInRange("127.127.255.255"));
        assertTrue(cidr.isInRange("127.127.0.0"));
        assertFalse(cidr.isInRange("127.126.255.255"));
        assertFalse(cidr.isInRange("127.128.0.0"));
    }

    @Test
    public void shouldHonorClassA() throws Exception
    {
        CIDR cidr = new CIDR("127.127.127.127/8");
        assertTrue(cidr.isInRange("127.255.255.255"));
        assertTrue(cidr.isInRange("127.0.0.0"));
        assertFalse(cidr.isInRange("126.255.255.255"));
        assertFalse(cidr.isInRange("128.0.0.0"));
    }

    @Test
    public void shouldHonorDefaultRoute() throws Exception
    {
        CIDR cidr = new CIDR("0.0.0.0/0");
        assertTrue(cidr.isInRange("127.127.127.127"));
        assertTrue(cidr.isInRange("127.127.127.128"));
        assertTrue(cidr.isInRange("127.127.127.126"));
        assertTrue(cidr.isInRange("255.255.255.255"));
        assertTrue(cidr.isInRange("0.0.0.0"));
        assertTrue(cidr.isInRange("0.0.0.127"));
    }

}
