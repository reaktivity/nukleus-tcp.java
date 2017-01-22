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
package org.reaktivity.nukleus.tcp.internal.writer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Predicate;

public class Route
{
    private final String source;
    private final long sourceRef;
    private final Target target;
    private final long targetRef;
    private final InetSocketAddress address;

    public Route(
        String source,
        long sourceRef,
        Target target,
        long targetRef,
        InetSocketAddress address)
    {
        this.source = source;
        this.sourceRef = sourceRef;
        this.target = target;
        this.targetRef = targetRef;
        this.address = address;
    }

    public String source()
    {
        return source;
    }

    public long sourceRef()
    {
        return sourceRef;
    }

    public Target target()
    {
        return target;
    }

    public long targetRef()
    {
        return this.targetRef;
    }

    public InetSocketAddress address()
    {
        return address;
    }

    @Override
    public int hashCode()
    {
        return address.hashCode();
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Route))
        {
            return false;
        }

        Route that = (Route) obj;
        return this.sourceRef == that.sourceRef &&
                this.targetRef == that.targetRef &&
                Objects.equals(this.source, that.source) &&
                Objects.equals(this.target, that.target) &&
                Objects.equals(this.address, that.address);
    }

    @Override
    public String toString()
    {
        return String.format("[source=\"%s\", sourceRef=%d, target=\"%s\", targetRef=%d, address=%s]",
                source, sourceRef, target.name(), targetRef, address);
    }

    public static Predicate<Route> sourceMatches(
        String source)
    {
        Objects.requireNonNull(source);
        return r -> source.equals(r.source);
    }

    public static Predicate<Route> sourceRefMatches(
        long sourceRef)
    {
        return r -> sourceRef == r.sourceRef;
    }

    public static Predicate<Route> targetMatches(
        String target)
    {
        Objects.requireNonNull(target);
        return r -> target.equals(r.target.name());
    }

    public static Predicate<Route> targetRefMatches(
        long targetRef)
    {
        return r -> targetRef == r.targetRef;
    }

    public static Predicate<Route> addressMatches(
        SocketAddress address)
    {
        // allow null address
        return r -> Objects.equals(address, r.address);
    }
}
