package org.reaktivity.nukleus.tcp.internal.streams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import org.jboss.byteman.rule.helper.Helper;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class SocketChannelHelper
{
    public static final TestRule RULE = new Rule();

    public static class ProcessDataHelper extends Helper
    {

        private static PrimitiveIterator.OfInt processData;

        protected ProcessDataHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public static void fragmentWrites(IntStream stream)
        {
            ProcessDataHelper.processData = stream.iterator();
        }

        public int doWrite(SocketChannel channel, ByteBuffer buffer) throws IOException
        {
            return write(channel, buffer, processData);
        }

        private static void reset()
        {
            processData = IntStream.empty().iterator();
        }
    }

    public static class HandleWriteHelper extends Helper
    {

        private static PrimitiveIterator.OfInt handleWrite;

        protected HandleWriteHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public static void fragmentWrites(IntStream stream)
        {
            HandleWriteHelper.handleWrite = stream.iterator();
        }

        public int doWrite(SocketChannel channel, ByteBuffer buffer) throws IOException
        {
            return write(channel, buffer, handleWrite);
        }

        private static void reset()
        {
            handleWrite = IntStream.empty().iterator();
        }
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

    private SocketChannelHelper()
    {

    }

    private static void reset()
    {
        ProcessDataHelper.reset();
        HandleWriteHelper.reset();
    }

    private static int write(SocketChannel channel, ByteBuffer b, PrimitiveIterator.OfInt iterator) throws IOException
    {
        int bytesToWrite = iterator.hasNext() ? iterator.nextInt() : -1;
        bytesToWrite = bytesToWrite == -1 ? b.remaining() : bytesToWrite;
        int limit = b.limit();
        b.limit(b.position() + bytesToWrite);
        int written = channel.write(b);
        b.limit(limit);
        return written;
    }

}