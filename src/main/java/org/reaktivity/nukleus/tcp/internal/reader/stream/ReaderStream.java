package org.reaktivity.nukleus.tcp.internal.reader.stream;

import static java.nio.channels.SelectionKey.OP_READ;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.reaktivity.nukleus.tcp.internal.poller.PollerKey;
import org.reaktivity.nukleus.tcp.internal.reader.Target;
import org.reaktivity.nukleus.tcp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.tcp.internal.types.stream.WindowFW;

final class ReaderStream
{
    private final ReaderStreamFactory readerStreamFactory;
    private final Target target;
    private final long streamId;
    private final PollerKey key;
    private final SocketChannel channel;
    private final long correlationId;

    private int readableBytes;

    ReaderStream(
        ReaderStreamFactory readerStreamFactory, Target target,
        long streamId,
        PollerKey key,
        SocketChannel channel,
        long correlationId)
    {
        this.readerStreamFactory = readerStreamFactory;
        this.target = target;
        this.streamId = streamId;
        this.key = key;
        this.channel = channel;
        this.correlationId = correlationId;
    }

    int handleStream(
        PollerKey key)
    {
        assert readableBytes > 0;

        final int limit = Math.min(readableBytes, this.readerStreamFactory.bufferSize);

        this.readerStreamFactory.readBuffer.position(0);
        this.readerStreamFactory.readBuffer.limit(limit);

        int bytesRead;
        try
        {
            bytesRead = channel.read(this.readerStreamFactory.readBuffer);
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
            target.doTcpEnd(streamId);
            target.removeThrottle(streamId);

            key.cancel(OP_READ);
        }
        else if (bytesRead != 0)
        {
            // atomic buffer is zero copy with read buffer
            target.doTcpData(streamId, this.readerStreamFactory.atomicBuffer, 0, bytesRead);

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
        this.readerStreamFactory.windowRO.wrap(buffer, index, index + length);

        if (readableBytes != -1)
        {
            final int update = this.readerStreamFactory.windowRO.update();

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
        this.readerStreamFactory.resetRO.wrap(buffer, index, index + length);

        try
        {
            if (this.readerStreamFactory.resolveCorrelation.apply(correlationId) == null)
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
        finally
        {
            target.removeThrottle(streamId);
        }
    }
}