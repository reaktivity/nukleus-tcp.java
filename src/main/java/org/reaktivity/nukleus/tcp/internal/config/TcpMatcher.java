/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.config;

import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.reaktivity.nukleus.tcp.internal.util.Cidr;

public final class TcpMatcher
{
    public final Cidr cidr;
    public final Matcher authority;

    public TcpMatcher(
        TcpCondition condition)
    {
        this.cidr = condition.cidr != null ? new Cidr(condition.cidr) : null;
        this.authority = condition.authority != null ? asMatcher(condition.authority) : null;
    }

    public boolean matches(
        InetAddress remote)
    {
        return matchesCidr(remote) &&
                matchesAuthority(remote);
    }

    private boolean matchesCidr(
        InetAddress remote)
    {
        return cidr == null || cidr.matches(remote);
    }

    private boolean matchesAuthority(
        InetAddress remote)
    {
        return authority == null || authority.reset(remote.getHostName()).matches();
    }

    private Matcher asMatcher(
        String pattern)
    {
        return Pattern.compile(pattern.replaceAll("\\*", ".*")).matcher("");
    }
}
