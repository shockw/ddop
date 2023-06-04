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

import com.reache.ddop.scheduler.common.enums.StateEventType;
import com.reache.ddop.scheduler.common.enums.TaskEventType;
import com.reache.ddop.scheduler.dao.entity.TaskInstance;
import com.reache.ddop.scheduler.dao.utils.TaskInstanceUtils;
import com.reache.ddop.scheduler.remote.command.TaskExecuteRunningAckMessage;
import com.reache.ddop.scheduler.service.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.processor.queue.TaskEvent;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteThreadPool;

import java.util.Optional;

@Component
public class TaskRunningEventHandler implements TaskEventHandler {

    private final Logger logger = LoggerFactory.getLogger(TaskRunningEventHandler.class);

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private WorkflowExecuteThreadPool workflowExecuteThreadPool;

    @Autowired
    private ProcessService processService;

    @Override
    public void handleTaskEvent(TaskEvent taskEvent) throws TaskEventHandleError {
        int taskInstanceId = taskEvent.getTaskInstanceId();
        int processInstanceId = taskEvent.getProcessInstanceId();

        WorkflowExecuteRunnable workflowExecuteRunnable =
                this.processInstanceExecCacheManager.getByProcessInstanceId(processInstanceId);
        if (workflowExecuteRunnable == null) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task running event error, cannot find related workflow instance from cache, will discard this event");
        }
        Optional<TaskInstance> taskInstanceOptional = workflowExecuteRunnable.getTaskInstance(taskInstanceId);
        if (!taskInstanceOptional.isPresent()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle running event error, cannot find the taskInstance from cache, will discord this event");
        }
        TaskInstance taskInstance = taskInstanceOptional.get();
        if (taskInstance.getState().isFinished()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task running event error, this task instance is already finished, this event is delay, will discard this event");
        }

        TaskInstance oldTaskInstance = new TaskInstance();
        TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
        try {
            taskInstance.setState(taskEvent.getState());
            taskInstance.setStartTime(taskEvent.getStartTime());
            taskInstance.setHost(taskEvent.getWorkerAddress());
            taskInstance.setLogPath(taskEvent.getLogPath());
            taskInstance.setExecutePath(taskEvent.getExecutePath());
            taskInstance.setPid(taskEvent.getProcessId());
            taskInstance.setAppLink(taskEvent.getAppIds());
            if (!processService.updateTaskInstance(taskInstance)) {
                throw new TaskEventHandleError("Handle task running event error, update taskInstance to db failed");
            }
            sendAckToWorker(taskEvent);
        } catch (Exception ex) {
            TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
            if (ex instanceof TaskEventHandleError) {
                throw ex;
            }
            throw new TaskEventHandleError("Handle task running event error, update taskInstance to db failed", ex);
        }

        TaskStateEvent stateEvent = TaskStateEvent.builder()
                .processInstanceId(taskEvent.getProcessInstanceId())
                .taskInstanceId(taskEvent.getTaskInstanceId())
                .status(taskEvent.getState())
                .type(StateEventType.TASK_STATE_CHANGE)
                .build();
        workflowExecuteThreadPool.submitStateEvent(stateEvent);
    }

    private void sendAckToWorker(TaskEvent taskEvent) {
        // If event handle success, send ack to worker to otherwise the worker will retry this event
        TaskExecuteRunningAckMessage taskExecuteRunningAckMessage =
                new TaskExecuteRunningAckMessage(true, taskEvent.getTaskInstanceId());
        taskEvent.getChannel().writeAndFlush(taskExecuteRunningAckMessage.convert2Command());
    }

    @Override
    public TaskEventType getHandleEventType() {
        return TaskEventType.RUNNING;
    }
}
