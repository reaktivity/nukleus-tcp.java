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

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.tcp.internal.types.Flyweight;
import org.reaktivity.nukleus.tcp.internal.types.control.ErrorFW;
import org.reaktivity.nukleus.tcp.internal.types.control.Role;
import org.reaktivity.nukleus.tcp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.RoutedFW;
import org.reaktivity.nukleus.tcp.internal.types.control.State;
import org.reaktivity.nukleus.tcp.internal.types.control.TcpRouteExFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.tcp.internal.types.control.UnroutedFW;


public final class TcpController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final TcpRouteExFW.Builder routeExRW = new TcpRouteExFW.Builder();

    private final ErrorFW errorRO = new ErrorFW();
    private final RoutedFW routedRO = new RoutedFW();
    private final UnroutedFW unroutedRO = new UnroutedFW();

    private final Context context;
    private final RingBuffer conductorCommands;
    private final CopyBroadcastReceiver conductorResponses;
    private final AtomicBuffer atomicBuffer;
    private final Long2ObjectHashMap<CompletableFuture<?>> promisesByCorrelationId;

    public TcpController(Context context)
    {
        this.context = context;
        this.conductorCommands = context.conductorCommands();
        this.conductorResponses = new CopyBroadcastReceiver(new BroadcastReceiver(context.conductorResponseBuffer()));
        this.atomicBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.promisesByCorrelationId = new Long2ObjectHashMap<>();
    }

    @Override
    public int process()
    {
        int weight = 0;

        weight += conductorResponses.receive(this::handleResponse);

        return weight;
    }

    @Override
    public void close() throws Exception
    {
        context.close();
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

    public CompletableFuture<Long> routeInputNone(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.INPUT, State.NONE, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeInputNew(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.INPUT, State.NEW, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeInputEstablished(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.INPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeOutputNone(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.OUTPUT, State.NONE, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeOutputtNew(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.OUTPUT, State.NEW, source, sourceRef, target, targetRef, address);
    }

    public CompletableFuture<Long> routeOutputEstablished(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
    {
        return route(Role.OUTPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, address);
    }

    private CompletableFuture<Long> route(
        Role role,
        State state,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        final CompletableFuture<Long> promise = new CompletableFuture<>();

        long correlationId = conductorCommands.nextCorrelationId();

        RouteFW routeRO = routeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .state(b -> b.set(state))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(b -> b.set(visitRouteEx(address)))
                                 .build();

        if (!conductorCommands.write(routeRO.typeId(), routeRO.buffer(), routeRO.offset(), routeRO.length()))
        {
            commandSendFailed(promise);
        }
        else
        {
            commandSent(correlationId, promise);
        }

        return promise;
    }

    public CompletableFuture<Void> unroute(
        Role role,
        State state,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        InetAddress address)
    {
        final CompletableFuture<Void> promise = new CompletableFuture<>();

        long correlationId = conductorCommands.nextCorrelationId();

        UnrouteFW unrouteRO = unrouteRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .state(b -> b.set(state))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(b -> b.set(visitRouteEx(address)))
                                 .build();

        if (!conductorCommands.write(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.length()))
        {
            commandSendFailed(promise);
        }
        else
        {
            commandSent(correlationId, promise);
        }

        return promise;
    }

    public TcpStreams streams(
        String source,
        String target)
    {
        return new TcpStreams(context, source, target);
    }

    private Flyweight.Builder.Visitor visitRouteEx(
        InetAddress address)
    {
        if (address == null)
        {
            return (buffer, offset, limit) -> routeExRW.wrap(buffer, offset, limit).build().length();
        }

        return (buffer, offset, limit) ->
            routeExRW.wrap(buffer, offset, limit)
                     .address(a -> inetAddress(address, a::ipv4Address, a::ipv6Address))
                     .build()
                     .length();
    }

    private int handleResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case ErrorFW.TYPE_ID:
            handleErrorResponse(buffer, index, length);
            break;
        case RoutedFW.TYPE_ID:
            handleRoutedResponse(buffer, index, length);
            break;
        case UnroutedFW.TYPE_ID:
            handleUnroutedResponse(buffer, index, length);
            break;
        default:
            break;
        }

        return 1;
    }

    private void handleErrorResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        errorRO.wrap(buffer, index, length);
        long correlationId = errorRO.correlationId();

        CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandFailed(promise, "command failed");
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRoutedResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        routedRO.wrap(buffer, index, length);
        long correlationId = routedRO.correlationId();
        final long sourceRef = routedRO.sourceRef();

        CompletableFuture<Long> promise = (CompletableFuture<Long>) promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandSucceeded(promise, sourceRef);
        }
    }

    private void handleUnroutedResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        unroutedRO.wrap(buffer, index, length);
        long correlationId = unroutedRO.correlationId();

        CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandSucceeded(promise);
        }
    }

    private void commandSent(
        final long correlationId,
        final CompletableFuture<?> promise)
    {
        promisesByCorrelationId.put(correlationId, promise);
    }

    private <T> boolean commandSucceeded(
        final CompletableFuture<T> promise)
    {
        return commandSucceeded(promise, null);
    }

    private <T> boolean commandSucceeded(
        final CompletableFuture<T> promise,
        final T value)
    {
        return promise.complete(value);
    }

    private boolean commandSendFailed(
        final CompletableFuture<?> promise)
    {
        return commandFailed(promise, "unable to offer command");
    }

    private boolean commandFailed(
        final CompletableFuture<?> promise,
        final String message)
    {
        return promise.completeExceptionally(new IllegalStateException(message).fillInStackTrace());
    }
}
