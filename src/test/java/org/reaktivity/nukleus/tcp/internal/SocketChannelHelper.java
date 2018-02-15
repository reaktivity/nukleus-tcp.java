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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.PrimitiveIterator;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.jboss.byteman.rule.helper.Helper;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class SocketChannelHelper
{
    public static final TestRule RULE = new Rule();
    public static final int ALL = -1;

    public static class OnTransferHelper extends Helper
    {

        private static PrimitiveIterator.OfInt onTransfer;

        protected OnTransferHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public static void fragmentWrites(IntStream stream)
        {
            OnTransferHelper.onTransfer = stream.iterator();
        }

        public int doWrite(SocketChannel channel, ByteBuffer buffer) throws IOException
        {
            return write(channel, buffer, onTransfer);
        }

        private static void reset()
        {
            onTransfer = IntStream.empty().iterator();
        }
    }

    public static class OnNotifyWritableHelper extends Helper
    {

        private static PrimitiveIterator.OfInt onNotifyWritable;

        protected OnNotifyWritableHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public static void fragmentWrites(IntStream stream)
        {
            OnNotifyWritableHelper.onNotifyWritable = stream.iterator();
        }

        public int doWrite(SocketChannel channel, ByteBuffer buffer) throws IOException
        {
            return write(channel, buffer, onNotifyWritable);
        }

        private static void reset()
        {
            onNotifyWritable = IntStream.empty().iterator();
        }
    }

    public static class CountDownHelper extends Helper
    {
        private static CountDownLatch latch;

        protected CountDownHelper(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public void countDown()
        {
            latch.countDown();
        }

        public static void initialize(CountDownLatch latch)
        {
            CountDownHelper.latch = latch;
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
        OnTransferHelper.reset();
        OnNotifyWritableHelper.reset();
    }

    private static int write(
        SocketChannel channel,
        ByteBuffer b,
        PrimitiveIterator.OfInt iterator) throws IOException
    {
        int bytesToWrite = iterator.hasNext() ? iterator.nextInt() : ALL;
        bytesToWrite = bytesToWrite == ALL ? b.remaining() : bytesToWrite;
        int limit = b.limit();
        b.limit(b.position() + bytesToWrite);
        int written = channel.write(b);
        b.limit(limit);
        return written;
    }

}
