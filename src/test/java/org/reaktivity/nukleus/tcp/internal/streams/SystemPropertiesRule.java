package org.reaktivity.nukleus.tcp.internal.streams;

import java.util.HashMap;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SystemPropertiesRule implements TestRule
{
    HashMap<String, String> properties = new HashMap<>();

    public SystemPropertiesRule setProperty(String propertyName, String value)
    {
        properties.put(propertyName, value);
        return this;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    properties.forEach((name, value) -> System.setProperty(name, value));
                    base.evaluate();
                }
                finally
                {
                    properties.keySet().forEach((name) -> System.clearProperty(name));
                }
            }

        };

    }


}
