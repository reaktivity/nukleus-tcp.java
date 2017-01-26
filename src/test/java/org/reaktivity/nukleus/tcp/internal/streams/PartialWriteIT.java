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
package org.reaktivity.nukleus.tcp.internal.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.instrument.Instrumentation;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

public class PartialWriteIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/tcp/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/tcp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("tcp")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .streams("tcp", "target");

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @BeforeClass
    public static void setupByteBuddyAgent()
    {
        Instrumentation instrumentation = ByteBuddyAgent.install();
        TestAgent.installOn(instrumentation, listener());
//        try
//        {
//            Selector.open();
//        }
//        catch (IOException e)
//        {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/server.sent.data/server/target"
    })
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final InputStream in = socket.getInputStream();

            byte[] buf = new byte[256];
            int len = in.read(buf);

            assertEquals("server data", new String(buf, 0, len, UTF_8));
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/client.sent.data/server/target"
    })
    public void shouldReceiveClientSentData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");

        try (Socket socket = new Socket("127.0.0.1", 0x1f90))
        {
            final OutputStream out = socket.getOutputStream();

            out.write("client data".getBytes());

            k3po.finish();
        }
    }

    public static final class TestAgent
    {
        public static void premain(
            String args,
            Instrumentation inst)
        {
            installOn(inst, AgentBuilder.Listener.StreamWriting.toSystemOut());
        }

        static void installOn(
            Instrumentation inst,
            AgentBuilder.Listener listener)
        {
            new AgentBuilder.Default()
              .with(listener)
              .type(ElementMatchers.isSubTypeOf(SocketChannel.class))
                      //.or(ElementMatchers.isSubTypeOf(ServerSocketChannel.class)))
              .transform(new SocketChannelTransformer())
              .installOn(inst);
        }

        private TestAgent()
        {
        }
    }

    private static final class SocketChannelTransformer implements AgentBuilder.Transformer
    {

        @Override
        public Builder<?> transform(Builder<?> builder, TypeDescription arg1, ClassLoader arg2, JavaModule arg3)
        { //hasDescriptor("(L ByteBuffer ;)I") .and(ElementMatchers.takesArguments(1))
            return builder.method(isPublic().and(named("write").and(ElementMatchers.takesArguments(1))))
                    // .intercept(FixedValue.value(3)); // blocks in Selector.open()
                    .intercept(MethodDelegation.to(Write.class));
                    //.intercept(SuperMethodCall.INSTANCE);// works
        }

    }

    private static final class WriteMethod implements Implementation
    {

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType)
        {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target arg0)
        {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class Write
    {
        static
        {
            System.out.println(Write.class.getName() + " loaded");
        }

        //@SuppressWarnings("unused") //@Origin Method zuper,
        public static int write(ByteBuffer buffer)
                throws IOException
        {
            System.out.println("Write " + buffer);
            return 0; //(Integer) zuper.invoke(buffer);
        }
    }

    private static AgentBuilder.Listener listener()
    {
        return new AgentBuilder.Listener.Adapter()
        {
            @Override
            public void onComplete(String arg0, ClassLoader arg1, JavaModule arg2, boolean arg3)
            {
                System.out.println("COMPLETE:" + arg0);
            }

            @Override
            public void onError(String typeName, ClassLoader arg1, JavaModule arg2, boolean arg3, Throwable throwable)
            {
                System.out.println("ERROR: " + typeName);
                throwable.printStackTrace(System.out);
            }

            @Override
            public void onIgnored(TypeDescription arg0, ClassLoader arg1, JavaModule arg2, boolean arg3)
            {
                System.out.println("IGNORED:" + arg0);
            }

            @Override
            public void onTransformation(TypeDescription typeDescription, ClassLoader loader,
                                         JavaModule arg2, boolean arg3, DynamicType dynamicType)
            {
//              try
//              {
//                  dynamicType.saveIn(new File("target/generated-classes/bytebuddy"));
//              }
//              catch (IOException e)
//              {
//                  // TODO Auto-generated catch block
//                  e.printStackTrace();
//              }
              System.out.println("TRANFORMED: " + typeDescription);
            }

        };
    }

}