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
