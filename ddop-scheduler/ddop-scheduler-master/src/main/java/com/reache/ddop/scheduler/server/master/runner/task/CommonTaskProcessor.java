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

package com.reache.ddop.scheduler.server.master.runner.task;

import com.google.auto.service.AutoService;
import com.reache.ddop.scheduler.server.master.dispatch.context.ExecutionContext;
import com.reache.ddop.scheduler.server.master.dispatch.enums.ExecutorType;
import com.reache.ddop.scheduler.server.master.dispatch.exceptions.ExecuteException;
import com.reache.ddop.scheduler.server.master.dispatch.executor.NettyExecutorManager;

import org.apache.commons.lang3.StringUtils;
import com.reache.ddop.scheduler.common.constants.Constants;
import com.reache.ddop.scheduler.common.constants.DataSourceConstants;
import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.plugin.task.api.TaskConstants;
import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.plugin.task.api.enums.TaskExecutionStatus;
import com.reache.ddop.scheduler.remote.command.TaskKillRequestCommand;
import com.reache.ddop.scheduler.remote.utils.Host;
import com.reache.ddop.scheduler.service.bean.SpringApplicationContext;
import com.reache.ddop.scheduler.service.queue.TaskPriority;
import com.reache.ddop.scheduler.service.queue.TaskPriorityQueue;
import com.reache.ddop.scheduler.service.queue.TaskPriorityQueueImpl;

import java.util.Date;

/**
 * common task processor
 */
@AutoService(ITaskProcessor.class)
public class CommonTaskProcessor extends BaseTaskProcessor {

    private TaskPriorityQueue<TaskPriority> taskUpdateQueue;

    private NettyExecutorManager nettyExecutorManager = SpringApplicationContext.getBean(NettyExecutorManager.class);

    @Override
    protected boolean submitTask() {
        this.taskInstance =
                processService.submitTaskWithRetry(processInstance, taskInstance, maxRetryTimes, commitInterval);

        return this.taskInstance != null;
    }

    @Override
    protected boolean resubmitTask() {
        if (this.taskInstance == null) {
            return false;
        }
        setTaskExecutionLogger();
        return dispatchTask();
    }

    @Override
    public boolean runTask() {
        return true;
    }

    @Override
    protected boolean taskTimeout() {
        return true;
    }

    /**
     * common task cannot be paused
     */
    @Override
    protected boolean pauseTask() {
        return true;
    }

    @Override
    public String getType() {
        return Constants.COMMON_TASK_TYPE;
    }

    @Override
    public boolean dispatchTask() {
        try {
            if (taskUpdateQueue == null) {
                this.initQueue();
            }
            if (taskInstance.getState().isFinished()) {
                logger.info("Task {} has already finished, no need to submit to task queue, taskState: {}",
                        taskInstance.getName(), taskInstance.getState());
                return true;
            }
            // task cannot be submitted because its execution state is RUNNING or DELAY.
            if (taskInstance.getState() == TaskExecutionStatus.RUNNING_EXECUTION
                    || taskInstance.getState() == TaskExecutionStatus.DELAY_EXECUTION) {
                logger.info("Task {} is already running or delayed, no need to submit to task queue, taskState: {}",
                        taskInstance.getName(), taskInstance.getState());
                return true;
            }
            logger.info("Task {} is ready to dispatch to worker", taskInstance.getName());

            TaskPriority taskPriority = new TaskPriority(processInstance.getProcessInstancePriority().getCode(),
                    processInstance.getId(), taskInstance.getProcessInstancePriority().getCode(),
                    taskInstance.getId(), taskInstance.getTaskGroupPriority(),
                    Constants.DEFAULT_WORKER_GROUP);

            TaskExecutionContext taskExecutionContext = getTaskExecutionContext(taskInstance);
            if (taskExecutionContext == null) {
                logger.error("Get taskExecutionContext fail, task: {}", taskInstance);
                return false;
            }

            taskPriority.setTaskExecutionContext(taskExecutionContext);

            taskUpdateQueue.put(taskPriority);
            logger.info("Task {} is submitted to priority queue success by master", taskInstance.getName());
            return true;
        } catch (Exception e) {
            logger.error("Task {} is submitted to priority queue error", taskInstance.getName(), e);
            return false;
        }
    }

    public void initQueue() {
        this.taskUpdateQueue = SpringApplicationContext.getBean(TaskPriorityQueueImpl.class);
    }

    @Override
    public boolean killTask() {

        try {
            logger.info("Begin to kill task: {}", taskInstance.getName());
            if (taskInstance == null) {
                logger.warn("Kill task failed, the task instance is not exist");
                return true;
            }
            if (taskInstance.getState().isFinished()) {
                logger.warn("Kill task failed, the task instance is already finished");
                return true;
            }
            // we don't wait the kill response
            taskInstance.setState(TaskExecutionStatus.KILL);
            taskInstance.setEndTime(new Date());
            processService.updateTaskInstance(taskInstance);
            if (StringUtils.isNotEmpty(taskInstance.getHost())) {
                killRemoteTask();
            }
        } catch (Exception e) {
            logger.error("Master kill task: {} error, taskInstance id: {}", taskInstance.getName(),
                    taskInstance.getId(), e);
            return false;
        }

        logger.info("Master success kill task: {}, taskInstanceId: {}", taskInstance.getName(), taskInstance.getId());
        return true;
    }

    private void killRemoteTask() throws ExecuteException {
        TaskKillRequestCommand killCommand = new TaskKillRequestCommand();
        killCommand.setTaskInstanceId(taskInstance.getId());

        ExecutionContext executionContext =
                new ExecutionContext(killCommand.convert2Command(), ExecutorType.WORKER, taskInstance);

        Host host = Host.of(taskInstance.getHost());
        executionContext.setHost(host);

        nettyExecutorManager.executeDirectly(executionContext);
    }
}
