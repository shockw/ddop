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

package com.reache.ddop.scheduler.plugin.task.pigeon;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reache.ddop.scheduler.plugin.task.api.model.ResourceInfo;
import com.reache.ddop.scheduler.plugin.task.api.parameters.AbstractParameters;

/**
 * TIS parameter
 */
public class PigeonParameters extends AbstractParameters {

    private static final Logger logger = LoggerFactory.getLogger(PigeonParameters.class);
    /**
     * Pigeon target job name
     */
    private String targetJobName;

    public String getTargetJobName() {
        return targetJobName;
    }

    public void setTargetJobName(String targetJobName) {
        this.targetJobName = targetJobName;
    }

    @Override
    public boolean checkParameters() {
        if (StringUtils.isBlank(this.targetJobName)) {
            logger.error("checkParameters faild targetJobName can not be null");
            return false;
        }
        return true;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return Collections.emptyList();
    }
}
