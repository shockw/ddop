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

package com.reache.ddop.scheduler.data.quality.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.ValidateResult;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;

/**
 * Component
 */
public interface Component {

    Config getConfig();

    ValidateResult validateConfig();

    default ValidateResult validate(List<String> requiredOptions) {
        List<String> nonExistsOptions = new ArrayList<>();
        requiredOptions.forEach(x -> {
            if (Boolean.FALSE.equals(getConfig().has(x))) {
                nonExistsOptions.add(x);
            }
        });

        if (!nonExistsOptions.isEmpty()) {
            return new ValidateResult(
                    false,
                    nonExistsOptions.stream().map(option ->
                            "[" + option + "]").collect(Collectors.joining(",")) + " is not exist");
        } else {
            return new ValidateResult(true, "");
        }
    }

    void prepare(SparkRuntimeEnvironment prepareEnv);
}
