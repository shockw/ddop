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

package com.reache.ddop.scheduler.plugin.task.mlflow;

import com.reache.ddop.scheduler.spi.params.base.ParamsOptions;
import com.reache.ddop.scheduler.spi.params.base.PluginParams;
import com.reache.ddop.scheduler.spi.params.base.Validate;
import com.reache.ddop.scheduler.spi.params.input.InputParam;
import com.reache.ddop.scheduler.spi.params.radio.RadioParam;

import java.util.ArrayList;
import java.util.List;

import com.google.auto.service.AutoService;
import com.reache.ddop.scheduler.plugin.task.api.TaskChannel;
import com.reache.ddop.scheduler.plugin.task.api.TaskChannelFactory;

@AutoService(TaskChannelFactory.class)
public class MlflowTaskChannelFactory implements TaskChannelFactory {
    @Override
    public TaskChannel create() {
        return new MlflowTaskChannel();
    }

    @Override
    public String getName() {
        return "MLFLOW";
    }

    @Override
    public List<PluginParams> getParams() {
        List<PluginParams> paramsList = new ArrayList<>();

        InputParam nodeName = InputParam.newBuilder("name", "$t('Node name')").addValidate(Validate.newBuilder().setRequired(true).build()).build();

        RadioParam runFlag = RadioParam.newBuilder("runFlag", "RUN_FLAG").addParamsOptions(new ParamsOptions("NORMAL", "NORMAL", false)).addParamsOptions(new ParamsOptions("FORBIDDEN", "FORBIDDEN", false)).build();

        paramsList.add(nodeName);
        paramsList.add(runFlag);
        return paramsList;
    }
}
