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

package com.reache.ddop.scheduler.remote.command;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.reache.ddop.scheduler.common.utils.JSONUtils;

/**
 *  command context
 */
public class CommandContext implements Serializable {

    private Map<String, String> items = new LinkedHashMap<>();

    public Map<String, String> getItems() {
        return items;
    }

    public void setItems(Map<String, String> items) {
        this.items = items;
    }

    public void put(String key, String value) {
        items.put(key, value);
    }

    public String get(String key) {
        return items.get(key);
    }

    public byte[] toBytes() {
        return JSONUtils.toJsonByteArray(this);
    }

    public static CommandContext valueOf(byte[] src) {
        return JSONUtils.parseObject(src, CommandContext.class);
    }
}
