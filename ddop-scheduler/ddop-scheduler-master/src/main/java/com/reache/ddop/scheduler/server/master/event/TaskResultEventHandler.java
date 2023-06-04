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
import com.reache.ddop.scheduler.remote.command.TaskExecuteAckCommand;
import com.reache.ddop.scheduler.service.process.ProcessService;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.config.MasterConfig;
import com.reache.ddop.scheduler.server.master.processor.queue.TaskEvent;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteThreadPool;
import com.reache.ddop.scheduler.server.master.utils.DataQualityResultOperator;

@Component
public class TaskResultEventHandler implements TaskEventHandler {

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private WorkflowExecuteThreadPool workflowExecuteThreadPool;

    @Autowired
    private DataQualityResultOperator dataQualityResultOperator;

    @Autowired
    private ProcessService processService;

    @Autowired
    private MasterConfig masterConfig;

    @Override
    public void handleTaskEvent(TaskEvent taskEvent) throws TaskEventHandleError, TaskEventHandleException {
        int taskInstanceId = taskEvent.getTaskInstanceId();
        int processInstanceId = taskEvent.getProcessInstanceId();

        WorkflowExecuteRunnable workflowExecuteRunnable = this.processInstanceExecCacheManager.getByProcessInstanceId(
                processInstanceId);
        if (workflowExecuteRunnable == null) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, cannot find related workflow instance from cache, will discard this event");
        }
        Optional<TaskInstance> taskInstanceOptional = workflowExecuteRunnable.getTaskInstance(taskInstanceId);
        if (!taskInstanceOptional.isPresent()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, cannot find the taskInstance from cache, will discord this event");
        }
        TaskInstance taskInstance = taskInstanceOptional.get();
        if (taskInstance.getState().isFinished()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, the task instance is already finished, will discord this event");
        }
        dataQualityResultOperator.operateDqExecuteResult(taskEvent, taskInstance);

        TaskInstance oldTaskInstance = new TaskInstance();
        TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
        try {
            taskInstance.setStartTime(taskEvent.getStartTime());
            taskInstance.setHost(taskEvent.getWorkerAddress());
            taskInstance.setLogPath(taskEvent.getLogPath());
            taskInstance.setExecutePath(taskEvent.getExecutePath());
            taskInstance.setPid(taskEvent.getProcessId());
            taskInstance.setAppLink(taskEvent.getAppIds());
            taskInstance.setState(taskEvent.getState());
            taskInstance.setEndTime(taskEvent.getEndTime());
            taskInstance.setVarPool(taskEvent.getVarPool());
            processService.changeOutParam(taskInstance);
            processService.updateTaskInstance(taskInstance);
            sendAckToWorker(taskEvent);
        } catch (Exception ex) {
            TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
            throw new TaskEventHandleError("Handle task result event error, save taskInstance to db error", ex);
        }
        TaskStateEvent stateEvent = TaskStateEvent.builder()
                .processInstanceId(taskEvent.getProcessInstanceId())
                .taskInstanceId(taskEvent.getTaskInstanceId())
                .status(taskEvent.getState())
                .type(StateEventType.TASK_STATE_CHANGE)
                .build();
        workflowExecuteThreadPool.submitStateEvent(stateEvent);

    }

    public void sendAckToWorker(TaskEvent taskEvent) {
        // we didn't set the receiver address, since the ack doen's need to retry
        TaskExecuteAckCommand taskExecuteAckMessage = new TaskExecuteAckCommand(true,
                taskEvent.getTaskInstanceId(),
                masterConfig.getMasterAddress(),
                taskEvent.getWorkerAddress(),
                System.currentTimeMillis());
        taskEvent.getChannel().writeAndFlush(taskExecuteAckMessage.convert2Command());
    }

    @Override
    public TaskEventType getHandleEventType() {
        return TaskEventType.RESULT;
    }
}
