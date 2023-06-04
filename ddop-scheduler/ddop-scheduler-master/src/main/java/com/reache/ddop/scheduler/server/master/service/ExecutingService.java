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

package com.reache.ddop.scheduler.server.master.service;

import com.reache.ddop.scheduler.dao.entity.TaskInstance;
import com.reache.ddop.scheduler.remote.dto.TaskInstanceExecuteDto;
import com.reache.ddop.scheduler.remote.dto.WorkflowExecuteDto;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.controller.WorkflowExecuteController;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;

/**
 * executing service, to query executing data from memory, such workflow instance
 */
@Component
public class ExecutingService {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecuteController.class);

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    public Optional<WorkflowExecuteDto> queryWorkflowExecutingData(Integer processInstanceId) {
        WorkflowExecuteRunnable workflowExecuteRunnable = processInstanceExecCacheManager.getByProcessInstanceId(processInstanceId);
        if (workflowExecuteRunnable == null) {
            logger.info("workflow execute data not found, maybe it has finished, workflow id:{}", processInstanceId);
            return Optional.empty();
        }
        try {
            WorkflowExecuteDto workflowExecuteDto = new WorkflowExecuteDto();
            BeanUtils.copyProperties(workflowExecuteDto, workflowExecuteRunnable.getProcessInstance());
            List<TaskInstanceExecuteDto> taskInstanceList = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(workflowExecuteRunnable.getAllTaskInstances())) {
                for (TaskInstance taskInstance : workflowExecuteRunnable.getAllTaskInstances()) {
                    TaskInstanceExecuteDto taskInstanceExecuteDto = new TaskInstanceExecuteDto();
                    BeanUtils.copyProperties(taskInstanceExecuteDto, taskInstance);
                    taskInstanceList.add(taskInstanceExecuteDto);
                }
            }
            workflowExecuteDto.setTaskInstances(taskInstanceList);
            return Optional.of(workflowExecuteDto);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("query workflow execute data fail, workflow id:{}", processInstanceId, e);
        }
        return Optional.empty();
    }
}
