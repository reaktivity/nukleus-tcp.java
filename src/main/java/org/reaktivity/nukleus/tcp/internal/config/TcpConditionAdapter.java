/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.tcp.internal.config;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.reaktivity.nukleus.tcp.internal.TcpNukleus;
import org.reaktivity.reaktor.config.Condition;
import org.reaktivity.reaktor.config.ConditionAdapterSpi;

public class TcpConditionAdapter implements ConditionAdapterSpi
{
    private static final String CIDR_NAME = "cidr";

    @Override
    public String type()
    {
        return TcpNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Condition condition)
    {
        TcpCondition tcpCondition = (TcpCondition) condition;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (tcpCondition.cidr != null)
        {
            object.add(CIDR_NAME, tcpCondition.cidr);
        }

        return object.build();
    }

    @Override
    public Condition adaptFromJson(
        JsonObject object)
    {
        String cidr = object.getString(CIDR_NAME, null);

        return new TcpCondition(cidr);
    }
}
