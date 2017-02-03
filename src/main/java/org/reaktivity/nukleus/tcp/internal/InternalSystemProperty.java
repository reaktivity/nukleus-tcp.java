/**
 * Copyright 2007-2016, Kaazing Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.reaktivity.nukleus.tcp.internal;

import java.util.Properties;
import java.util.function.IntSupplier;

public enum InternalSystemProperty
{

    // Maximum window size for nuklei writing data to the tcp nukleus
    WINDOW_SIZE("nukleus.tcp.window.size", "8192"),

    MAXIMUM_STREAMS_WITH_PENDING_WRITES("nukleus.tcp.maximum.pending.write.streams");

    private final String name;
    private final String defaultValue;

    InternalSystemProperty(String propertyName)
    {
        this(propertyName, null);
    }

    InternalSystemProperty(String name, String defaultValue)
    {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String stringValue(Properties configuration)
    {
        return System.getProperty(name, defaultValue);
    }

    public Integer intValue()
    {
        return Integer.getInteger(name, Integer.parseInt(defaultValue));
    }

    public Integer intValue(IntSupplier defaultValue)
    {
        return Integer.getInteger(name, defaultValue.getAsInt());
    }

    public String propertyName()
    {
        return name;
    }

}
