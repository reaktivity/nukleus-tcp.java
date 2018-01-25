package org.reaktivity.nukleus.tcp.internal;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.reaktor.test.ReaktorRule;

public class TcpFrameAndBytesCountersRule implements TestRule
{
    private final ReaktorRule reaktor;
    private TcpController controller;

    public TcpFrameAndBytesCountersRule(ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                controller = reaktor.controller(TcpController.class);
                base.evaluate();
            }
        };
    }

    public long bytesRead(long routeId)
    {
        return controller.count(routeId + ".bytes.read");
    }

    public long bytesWrote(long routeId)
    {
        return controller.count(routeId + ".bytes.wrote");
    }

    public long framesRead(long routeId)
    {
        return controller.count(routeId + ".frames.read");
    }

    public long framesWrote(long routeId)
    {
        return controller.count(routeId + ".frames.wrote");
    }
}
