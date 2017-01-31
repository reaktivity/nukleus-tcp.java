package org.reaktivity.nukleus.tcp.internal.streams;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.jboss.byteman.rule.helper.Helper;

/**
 * A <a href="http://byteman.jboss.org/downloads.html">Byteman</a> Helper used to simulate partial writes at
 * the TCP level for testing purposes. It is used in conjunction with a ByteMan script (.btm) file.
 */
public class PartialWriteBytemanHelper extends Helper
{
    public static Queue<Integer> writeResults;
    private static int oldLimit;

    protected PartialWriteBytemanHelper(org.jboss.byteman.rule.Rule rule)
    {
        super(rule);
    }

    public void preWrite(ByteBuffer b)
    {
        if (!callerEquals("org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData",
                true, true))
        {
            return;
        }
        Integer toWrite = writeResults.peek();
        if (toWrite != null)
        {
            oldLimit = b.limit();
            int originalBytesToWrite = b.limit() - b.position();
            debug(format("preWrite: forcing partial write for buffer %s, writing %d bytes of %d, changing limit to %d",
                    b, toWrite, originalBytesToWrite, b.position() + toWrite));
            b.limit(b.position() + toWrite);
        }
    }

    public void postWrite(ByteBuffer b, int returnValue)
    {
        if (!callerEquals("org.reaktivity.nukleus.tcp.internal.writer.stream.StreamFactory$Stream.processData",
                true, true))
        {
            return;
        }
        Integer toWrite = writeResults.poll();
        if (toWrite != null)
        {
            debug(format("postWrite: buffer after write is: %s, return value is %d, setting limit back to %d",
                    b, returnValue, oldLimit));
            b.limit(oldLimit);
        }
    }

    static void addWriteResult(Integer writeResult)
    {
        writeResults.add(writeResult);
    }

    static void initialize()
    {
        writeResults = new ArrayDeque<>(20);
    }

}