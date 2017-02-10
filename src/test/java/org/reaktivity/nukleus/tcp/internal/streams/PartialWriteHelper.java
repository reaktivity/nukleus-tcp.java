package org.reaktivity.nukleus.tcp.internal.streams;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import org.jboss.byteman.rule.helper.Helper;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A <a href="http://byteman.jboss.org/downloads.html">Byteman</a> Helper used to simulate partial writes at
 * the TCP level for testing purposes. It is used in conjunction with a ByteMan script (.btm) file.
 */
public class PartialWriteHelper extends Helper
{
    public static final TestRule RULE = new Rule();

    private static Queue<Integer> writeResults;
    private static Supplier<Integer> writeResultSupplier = () -> null;
    private static List<String> callers;
    private static Integer oldLimit;

    private static ByteBuffer b;

    public PartialWriteHelper(org.jboss.byteman.rule.Rule rule)
    {
        super(rule);
    }

    public void preWrite(String caller, Object[] parameters)
    {
        b = (ByteBuffer) parameters[1];
        callers.add(caller);
        Integer toWrite = writeResults.peek() != null ? writeResults.poll() : writeResultSupplier.get();
        if (toWrite != null)
        {
            oldLimit = b.limit();
            int originalBytesToWrite = b.limit() - b.position();
            toWrite = Math.min(originalBytesToWrite, toWrite);
            debug(format("preWrite for %s: forcing partial write for buffer %s, writing %d bytes of %d, changing limit to %d",
                    caller, b, toWrite, originalBytesToWrite, b.position() + toWrite));
            b.limit(b.position() + toWrite);
        }
        else
        {
            debug(format("preWrite for %s: normal write for buffer %s", caller, b));
            oldLimit = null;
        }
    }

    public void postWrite(int returnValue)
    {
        if (oldLimit != null)
        {
            debug(format("postWrite: buffer after write is: %s, returnValue %d, setting limit back to %d",
                    b, returnValue, oldLimit));
            b.limit(oldLimit);
        }
        b = null;
    }

    public static List<String> callers()
    {
        return callers;
    }

    static void addWriteResult(Integer writeResult)
    {
        writeResults.add(writeResult);
    }

    static void setWriteResultSupplier(Supplier<Integer> supplier)
    {
        writeResultSupplier = supplier;
    }

    private static void reset()
    {
        writeResults = new ArrayDeque<>(20);
        callers = new ArrayList<>(20);
        writeResultSupplier = () -> null;
    }

    private static class Rule implements TestRule
    {

        @Override
        public Statement apply(Statement base, Description description)
        {
            return new Statement()
            {

                @Override
                public void evaluate() throws Throwable
                {
                    reset();
                    base.evaluate();
                }

            };
        }

    }

}
