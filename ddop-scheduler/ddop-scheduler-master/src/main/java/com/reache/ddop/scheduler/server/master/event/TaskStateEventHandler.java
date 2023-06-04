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
import com.reache.ddop.scheduler.dao.entity.TaskInstance;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.reache.ddop.scheduler.server.master.metrics.TaskMetrics;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;
import com.reache.ddop.scheduler.server.master.runner.task.ITaskProcessor;
import com.reache.ddop.scheduler.server.master.runner.task.TaskAction;

@AutoService(StateEventHandler.class)
public class TaskStateEventHandler implements StateEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskStateEventHandler.class);

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable,
                                    StateEvent stateEvent) throws StateEventHandleException, StateEventHandleError {
        TaskStateEvent taskStateEvent = (TaskStateEvent) stateEvent;
        measureTaskState(taskStateEvent);
        workflowExecuteRunnable.checkTaskInstanceByStateEvent(taskStateEvent);

        Optional<TaskInstance> taskInstanceOptional =
                workflowExecuteRunnable.getTaskInstance(taskStateEvent.getTaskInstanceId());

        TaskInstance task = taskInstanceOptional.orElseThrow(() -> new StateEventHandleError(
                "Cannot find task instance from taskMap by task instance id: " + taskStateEvent.getTaskInstanceId()));

        if (task.getState() == null) {
            throw new StateEventHandleError("Task state event handle error due to task state is null");
        }

        logger.info(
                "Handle task instance state event, the current task instance state {} will be changed to {}",
                task.getState(), taskStateEvent.getStatus());

        Map<Long, Integer> completeTaskMap = workflowExecuteRunnable.getCompleteTaskMap();
        if (task.getState().isFinished()
                && (taskStateEvent.getStatus() != null && taskStateEvent.getStatus().isRunning())) {
            String errorMessage = String.format(
                    "The current task instance state is %s, but the task state event status is %s, so the task state event will be ignored",
                    task.getState(),
                    taskStateEvent.getStatus());
            logger.warn(errorMessage);
            throw new StateEventHandleError(errorMessage);
        }

        if (task.getState().isFinished()) {
            if (completeTaskMap.containsKey(task.getTaskCode())
                    && completeTaskMap.get(task.getTaskCode()).equals(task.getId())) {
                    logger.warn("The task instance is already complete, stateEvent: {}", stateEvent);
                return true;
            }
            workflowExecuteRunnable.taskFinished(task);
            if (task.getTaskGroupId() > 0) {
                logger.info("The task instance need to release task Group: {}", task.getTaskGroupId());
                workflowExecuteRunnable.releaseTaskGroup(task);
            }
            return true;
        }
        Map<Long, ITaskProcessor> activeTaskProcessMap = workflowExecuteRunnable.getActiveTaskProcessMap();
        if (activeTaskProcessMap.containsKey(task.getTaskCode())) {
            ITaskProcessor iTaskProcessor = activeTaskProcessMap.get(task.getTaskCode());
            iTaskProcessor.action(TaskAction.RUN);

            if (iTaskProcessor.taskInstance().getState().isFinished()) {
                if (iTaskProcessor.taskInstance().getState() != task.getState()) {
                    task.setState(iTaskProcessor.taskInstance().getState());
                }
                workflowExecuteRunnable.taskFinished(task);
            }
            return true;
        }
        throw new StateEventHandleError(
                "Task state event handle error, due to the task is not in activeTaskProcessorMaps");
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.TASK_STATE_CHANGE;
    }

    private void measureTaskState(TaskStateEvent taskStateEvent) {
        if (taskStateEvent == null || taskStateEvent.getStatus() == null) {
            // the event is broken
            logger.warn("The task event is broken..., taskEvent: {}", taskStateEvent);
            return;
        }
        if (taskStateEvent.getStatus().isFinished()) {
            TaskMetrics.incTaskInstanceByState("finish");
        }
        switch (taskStateEvent.getStatus()) {
            case KILL:
                TaskMetrics.incTaskInstanceByState("stop");
                break;
            case SUCCESS:
                TaskMetrics.incTaskInstanceByState("success");
                break;
            case FAILURE:
                TaskMetrics.incTaskInstanceByState("fail");
                break;
            default:
                break;
        }
    }
}
