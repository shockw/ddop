/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reache.ddop.scheduler.dao.entity;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.reache.ddop.scheduler.dao.entity.UdfFunc.UdfFuncDeserializer;

public class UdfFuncTest {

    /**
     * test UdfFuncDeserializer.deserializeKey
     *
     * @throws IOException
     */
    @Test
    public void testUdfFuncDeserializer() throws IOException {

        // UdfFuncDeserializer.deserializeKey key is null
        UdfFuncDeserializer udfFuncDeserializer = new UdfFuncDeserializer();
        Assert.assertNull(udfFuncDeserializer.deserializeKey(null, null));

        //
        UdfFunc udfFunc = new UdfFunc();
        udfFunc.setResourceName("dolphin_resource_update");
        udfFunc.setResourceId(2);
        udfFunc.setClassName("com.reache.ddop.scheduler.test.mrUpdate");

        Assert.assertNotNull(udfFuncDeserializer.deserializeKey(udfFunc.toString(), null));
    }

}