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

package com.reache.ddop.scheduler.server.master.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MasterServerMetrics {

    /**
     * Used to measure the master server is overload.
     */
    private final Counter masterOverloadCounter =
            Counter.builder("ds.master.overload.count")
                    .description("Master server overload count")
                    .register(Metrics.globalRegistry);

    /**
     * Used to measure the number of process command consumed by master.
     */
    private final Counter masterConsumeCommandCounter =
            Counter.builder("ds.master.consume.command.count")
                    .description("Master server consume command count")
                    .register(Metrics.globalRegistry);

    public void incMasterOverload() {
        masterOverloadCounter.increment();
    }

    public void incMasterConsumeCommand(int commandCount) {
        masterConsumeCommandCounter.increment(commandCount);
    }

}
