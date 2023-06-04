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

package com.reache.ddop.scheduler.server.master.event;

import com.reache.ddop.scheduler.common.enums.TaskEventType;
import com.reache.ddop.scheduler.dao.entity.TaskInstance;
import com.reache.ddop.scheduler.dao.utils.TaskInstanceUtils;
import com.reache.ddop.scheduler.plugin.task.api.enums.TaskExecutionStatus;
import com.reache.ddop.scheduler.service.process.ProcessService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.processor.queue.TaskEvent;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;

@Component
public class TaskDispatchEventHandler implements TaskEventHandler {

    private final Logger logger = LoggerFactory.getLogger(TaskDispatchEventHandler.class);

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private ProcessService processService;

    @Override
    public void handleTaskEvent(TaskEvent taskEvent) throws TaskEventHandleError {
        int taskInstanceId = taskEvent.getTaskInstanceId();
        int processInstanceId = taskEvent.getProcessInstanceId();

        WorkflowExecuteRunnable workflowExecuteRunnable =
                this.processInstanceExecCacheManager.getByProcessInstanceId(processInstanceId);
        if (workflowExecuteRunnable == null) {
            throw new TaskEventHandleError("Cannot find related workflow instance from cache");
        }
        TaskInstance taskInstance = workflowExecuteRunnable.getTaskInstance(taskInstanceId)
                .orElseThrow(() -> new TaskEventHandleError("Cannot find related taskInstance from cache"));
        if (taskInstance.getState() != TaskExecutionStatus.SUBMITTED_SUCCESS) {
            logger.warn(
                    "The current taskInstance status is not SUBMITTED_SUCCESS, so the dispatch event will be discarded, the current is a delay event, event: {}",
                    taskEvent);
            return;
        }

        // todo: we need to just log the old status and rollback these two field, no need to copy all fields
        TaskInstance oldTaskInstance = new TaskInstance();
        TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
        // update the taskInstance status
        taskInstance.setState(TaskExecutionStatus.DISPATCH);
        taskInstance.setHost(taskEvent.getWorkerAddress());
        try {
            if (!processService.updateTaskInstance(taskInstance)) {
                throw new TaskEventHandleError("Handle task dispatch event error, update taskInstance to db failed");
            }
        } catch (Exception ex) {
            // rollback status
            TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
            if (ex instanceof TaskEventHandleError) {
                throw ex;
            }
            throw new TaskEventHandleError("Handle task running event error, update taskInstance to db failed", ex);
        }
    }

    @Override
    public TaskEventType getHandleEventType() {
        return TaskEventType.DISPATCH;
    }
}
