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
package org.reaktivity.nukleus.tcp.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.inetAddress;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.TcpRouteExFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;


public final class TcpController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final TcpRouteExFW.Builder routeExRW = new TcpRouteExFW.Builder();

    private final ControllerSpi controllerSpi;
    private final AtomicBuffer atomicBuffer;

    public TcpController(ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.atomicBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<TcpController> kind()
    {
        return TcpController.class;
    }

    @Override
    public String name()
    {
        return "tcp";
    }

    public CompletableFuture<Long> routeServer(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        return route(Role.SERVER, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeClient(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        return route(Role.CLIENT, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Void> unrouteServer(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        return unroute(Role.SERVER, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Void> unrouteClient(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        return unroute(Role.CLIENT, source, sourceRef, target, targetRef, address);
    }

    public long count(String name)
    {
        return controllerSpi.doCount(name);
    }

    private Flyweight.Builder.Visitor visitRouteEx(
        InetAddress address)
    {
        if (address == null)
        {
            return (buffer, offset, limit) -> routeExRW.wrap(buffer, offset, limit).build().sizeof();
        }

        return (buffer, offset, limit) ->
            routeExRW.wrap(buffer, offset, limit)
                     .address(a -> inetAddress(address, a::ipv4Address, a::ipv6Address))
                     .build()
                     .sizeof();
    }

    private CompletableFuture<Long> route(
        Role role,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW routeRO = routeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(b -> b.set(visitRouteEx(address)))
                                 .build();

        return controllerSpi.doRoute(routeRO.typeId(), routeRO.buffer(), routeRO.offset(), routeRO.sizeof());
    }

    private CompletableFuture<Void> unroute(
        Role role,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unrouteRO = unrouteRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(b -> b.set(visitRouteEx(address)))
                                 .build();

        return controllerSpi.doUnroute(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.sizeof());
    }

}
