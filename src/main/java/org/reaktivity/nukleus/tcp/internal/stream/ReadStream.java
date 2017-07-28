package org.reaktivity.nukleus.tcp.internal.stream;

import static java.nio.channels.SelectionKey.OP_READ;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

final class ReadStream
{
    private final MessageConsumer target;
    private final long streamId;
    private final PollerKey key;
    private final SocketChannel channel;
    private final long correlationId;
    private final Long2ObjectHashMap<?> correlations;
    private final ByteBuffer readBuffer;
    private final MutableDirectBuffer atomicBuffer;
    private final MessageWriter writer;

    private int readableBytes;

    ReadStream(
        MessageConsumer target,
        long streamId,
        PollerKey key,
        SocketChannel channel,
        long correlationId,
        Long2ObjectHashMap<?> correlations,
        ByteBuffer readByteBuffer,
        MutableDirectBuffer readBuffer,
        MessageWriter writer)
    {
        this.target = target;
        this.streamId = streamId;
        this.key = key;
        this.channel = channel;
        this.correlationId = correlationId;
        this.correlations = correlations;
        this.readBuffer = readByteBuffer;
        this.atomicBuffer = readBuffer;
        this.writer = writer;
    }

    int handleStream(
        PollerKey key)
    {
        assert readableBytes > 0;

        final int limit = Math.min(readableBytes, readBuffer.capacity());

        readBuffer.position(0);
        readBuffer.limit(limit);

        int bytesRead;
        try
        {
            bytesRead = channel.read(readBuffer);
        }
        catch(IOException ex)
        {
            // treat TCP reset as end-of-stream
            bytesRead = -1;
        }

        if (bytesRead == -1)
        {
            // channel input closed
            readableBytes = -1;
            writer.doTcpEnd(target, streamId);

            key.cancel(OP_READ);
        }
        else if (bytesRead != 0)
        {
            // atomic buffer is zero copy with read buffer
            writer.doTcpData(target, streamId, atomicBuffer, 0, bytesRead);

            readableBytes -= bytesRead;

            if (readableBytes == 0)
            {
                key.clear(OP_READ);
            }
        }

        return 1;
    }

    void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            processWindow(buffer, index, length);
            break;
        case ResetFW.TYPE_ID:
            processReset(buffer, index, length);
            break;
        default:
            // ignore
            break;
        }
    }

    private void processWindow(
        DirectBuffer buffer,
        int index,
        int length)
    {
        writer.windowRO.wrap(buffer, index, index + length);

        if (readableBytes != -1)
        {
            final int update = writer.windowRO.update();

            readableBytes += update;

            handleStream(key);

            if (readableBytes > 0)
            {
                key.register(OP_READ);
            }
        }
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        writer.resetRO.wrap(buffer, index, index + length);

        try
        {
            if (correlations.remove(correlationId) == null)
            {
                // Begin on correlated output stream was already processed
                channel.shutdownInput();
            }
            else
            {
                // Force a hard reset (TCP RST), as documented in "Orderly Versus Abortive Connection Release in Java"
                // (https://docs.oracle.com/javase/8/docs/technotes/guides/net/articles/connection_release.html)
                channel.setOption(StandardSocketOptions.SO_LINGER, 0);
                channel.close();
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}