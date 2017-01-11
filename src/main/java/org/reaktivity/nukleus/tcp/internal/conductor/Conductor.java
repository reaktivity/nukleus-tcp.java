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
package org.reaktivity.nukleus.tcp.internal.conductor;

import static org.reaktivity.nukleus.tcp.internal.util.IpUtil.inetAddress;

import java.net.InetSocketAddress;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.tcp.internal.Context;
import org.reaktivity.nukleus.tcp.internal.router.Router;
import org.reaktivity.nukleus.tcp.internal.types.OctetsFW;
import org.reaktivity.nukleus.tcp.internal.types.control.BindFW;
import org.reaktivity.nukleus.tcp.internal.types.control.BoundFW;
import org.reaktivity.nukleus.tcp.internal.types.control.ErrorFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RoutedFW;
import org.reaktivity.nukleus.tcp.internal.types.control.TcpRouteExFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnbindFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnboundFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnroutedFW;

@Reaktive
public final class Conductor implements Nukleus
{
    private static final int SEND_BUFFER_CAPACITY = 1024; // TODO: Configuration and Context

    private final BindFW bindRO = new BindFW();
    private final UnbindFW unbindRO = new UnbindFW();
    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final TcpRouteExFW routeExRO = new TcpRouteExFW();

    private final ErrorFW.Builder errorRW = new ErrorFW.Builder();
    private final BoundFW.Builder boundRW = new BoundFW.Builder();
    private final UnboundFW.Builder unboundRW = new UnboundFW.Builder();
    private final RoutedFW.Builder routedRW = new RoutedFW.Builder();
    private final UnroutedFW.Builder unroutedRW = new UnroutedFW.Builder();

    private final RingBuffer conductorCommands;

    private final BroadcastTransmitter conductorResponses;
    private final AtomicBuffer sendBuffer;

    private Router router;

    public Conductor(Context context)
    {
        this.conductorCommands = context.conductorCommands();
        this.conductorResponses = context.conductorResponses();

        this.sendBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
    }

    public void setRouter(
        Router router)
    {
        this.router = router;
    }

    @Override
    public int process()
    {
        return conductorCommands.read(this::handleCommand);
    }

    @Override
    public String name()
    {
        return "conductor";
    }

    public void onErrorResponse(long correlationId)
    {
        ErrorFW errorRO = errorRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                 .correlationId(correlationId)
                                 .build();

        conductorResponses.transmit(errorRO.typeId(), errorRO.buffer(), errorRO.offset(), errorRO.length());
    }

    public void onBoundResponse(
        long correlationId,
        long referenceId)
    {
        BoundFW boundRO = boundRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                 .correlationId(correlationId)
                                 .referenceId(referenceId)
                                 .build();

        conductorResponses.transmit(boundRO.typeId(), boundRO.buffer(), boundRO.offset(), boundRO.length());
    }

    public void onUnboundResponse(
        long correlationId)
    {
        UnboundFW unboundRO = unboundRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                       .correlationId(correlationId)
                                       .build();

        conductorResponses.transmit(unboundRO.typeId(), unboundRO.buffer(), unboundRO.offset(), unboundRO.length());
    }

    public void onRoutedResponse(
        long correlationId)
    {
        RoutedFW routedRO = routedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                    .correlationId(correlationId)
                                    .build();

        conductorResponses.transmit(routedRO.typeId(), routedRO.buffer(), routedRO.offset(), routedRO.length());
    }

    public void onUnroutedResponse(
        long correlationId)
    {
        UnroutedFW unroutedRO = unroutedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                          .correlationId(correlationId)
                                          .build();

        conductorResponses.transmit(unroutedRO.typeId(), unroutedRO.buffer(), unroutedRO.offset(), unroutedRO.length());
    }

    private void handleCommand(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        switch (msgTypeId)
        {
        case BindFW.TYPE_ID:
            handleBindCommand(buffer, index, length);
            break;
        case UnbindFW.TYPE_ID:
            handleUnbindCommand(buffer, index, length);
            break;
        case RouteFW.TYPE_ID:
            handleRouteCommand(buffer, index, length);
            break;
        case UnrouteFW.TYPE_ID:
            handleUnrouteCommand(buffer, index, length);
            break;
        default:
            // ignore unrecognized commands (forwards compatible)
            break;
        }
    }

    private void handleBindCommand(
        DirectBuffer buffer,
        int index,
        int length)
    {
        bindRO.wrap(buffer, index, index + length);

        final long correlationId = bindRO.correlationId();
        final int kind = bindRO.kind();

        router.doBind(correlationId, kind);
    }

    private void handleUnbindCommand(
        DirectBuffer buffer,
        int index,
        int length)
    {
        unbindRO.wrap(buffer, index, index + length);

        final long correlationId = unbindRO.correlationId();
        final long referenceId = unbindRO.referenceId();

        router.doUnbind(correlationId, referenceId);
    }

    private void handleRouteCommand(
        DirectBuffer buffer,
        int index,
        int length)
    {
        routeRO.wrap(buffer, index, index + length);

        final long correlationId = routeRO.correlationId();
        final String source = routeRO.source().asString();
        final long sourceRef = routeRO.sourceRef();
        final String target = routeRO.target().asString();
        final long targetRef = routeRO.targetRef();
        final OctetsFW extension = routeRO.extension();

        final TcpRouteExFW routeEx = extension.get(routeExRO::wrap);

        final InetSocketAddress address = new InetSocketAddress(inetAddress(routeEx.address()), routeEx.port());

        router.doRoute(correlationId, source, sourceRef, target, targetRef, address);
    }

    private void handleUnrouteCommand(
        DirectBuffer buffer,
        int index,
        int length)
    {
        unrouteRO.wrap(buffer, index, index + length);

        final long correlationId = unrouteRO.correlationId();
        final String source = unrouteRO.source().asString();
        final long sourceRef = unrouteRO.sourceRef();
        final String target = unrouteRO.target().asString();
        final long targetRef = unrouteRO.targetRef();
        final OctetsFW extension = unrouteRO.extension();

        final TcpRouteExFW unrouteEx = extension.get(routeExRO::wrap);
        final InetSocketAddress address = new InetSocketAddress(inetAddress(unrouteEx.address()), unrouteEx.port());

        router.doUnroute(correlationId, source, sourceRef, target, targetRef, address);
    }
}
