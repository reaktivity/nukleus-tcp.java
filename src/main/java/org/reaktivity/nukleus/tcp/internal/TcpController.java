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
package org.reaktivity.nukleus.tcp.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.concurrent.CompletableFuture;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.tcp.internal.types.control.FreezeFW;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;

public final class TcpController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
    private final FreezeFW.Builder freezeRW = new FreezeFW.Builder();

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
        String localAddress,
        String remoteAddress)
    {
        return route(Role.SERVER, localAddress, remoteAddress);
    }

    public CompletableFuture<Long> routeClient(
        String localAddress,
        String remoteAddress)
    {
        return route(Role.CLIENT, localAddress, remoteAddress);
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unrouteRO = unrouteRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .nukleus(name())
                                 .routeId(routeId)
                                 .build();

        return controllerSpi.doUnroute(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.sizeof());
    }

    public CompletableFuture<Void> freeze()
    {
        long correlationId = controllerSpi.nextCorrelationId();

        FreezeFW freeze = freezeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                  .correlationId(correlationId)
                                  .nukleus(name())
                                  .build();

        return controllerSpi.doFreeze(freeze.typeId(), freeze.buffer(), freeze.offset(), freeze.sizeof());
    }

    private CompletableFuture<Long> route(
        Role role,
        String localAddress,
        String remoteAddress)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW routeRO = routeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .nukleus(name())
                                 .role(b -> b.set(role))
                                 .localAddress(localAddress)
                                 .remoteAddress(remoteAddress)
                                 .build();

        return controllerSpi.doRoute(routeRO.typeId(), routeRO.buffer(), routeRO.offset(), routeRO.sizeof());
    }
}
