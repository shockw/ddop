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

package com.reache.ddop.scheduler.plugin.task.seatunnel.self;

import com.reache.ddop.scheduler.common.utils.JSONUtils;

import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.plugin.task.seatunnel.Constants;
import com.reache.ddop.scheduler.plugin.task.seatunnel.SeatunnelTask;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

public class SeatunnelEngineTask extends SeatunnelTask {

    private SeatunnelEngineParameters seatunnelParameters;
    public SeatunnelEngineTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
    }

    @Override
    public void init() {
        seatunnelParameters =
                JSONUtils.parseObject(taskExecutionContext.getTaskParams(), SeatunnelEngineParameters.class);
        setSeatunnelParameters(seatunnelParameters);
        super.init();
    }

    @Override
    public List<String> buildOptions() throws Exception {
        List<String> args = super.buildOptions();
        if (!Objects.isNull(seatunnelParameters.getDeployMode())) {
            args.add(Constants.DEPLOY_MODE_OPTIONS);
            args.add(seatunnelParameters.getDeployMode().getCommand());
        }
        if (StringUtils.isNotBlank(seatunnelParameters.getOthers())) {
            args.add(seatunnelParameters.getOthers());
        }
        return args;
    }

}
