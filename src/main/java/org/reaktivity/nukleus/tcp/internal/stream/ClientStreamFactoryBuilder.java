package org.reaktivity.nukleus.tcp.internal.stream;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public class ClientStreamFactoryBuilder implements StreamFactoryBuilder
{

    public ClientStreamFactoryBuilder(Configuration config)
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    public StreamFactory build()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(Supplier<BufferPool> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamFactoryBuilder setCorrelationIdSupplier(LongSupplier arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamFactoryBuilder setRouteHandler(RouteHandler arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamFactoryBuilder setStreamIdSupplier(LongSupplier arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamFactoryBuilder setWriteBuffer(MutableDirectBuffer arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
