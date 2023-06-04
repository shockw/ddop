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

package com.reache.ddop.scheduler.server.worker.runner;

import com.reache.ddop.scheduler.plugin.task.api.TaskCallBack;
import com.reache.ddop.scheduler.plugin.task.api.TaskConstants;
import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContextCacheManager;
import com.reache.ddop.scheduler.plugin.task.api.model.ApplicationInfo;
import com.reache.ddop.scheduler.remote.command.CommandType;

import lombok.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reache.ddop.scheduler.server.worker.rpc.WorkerMessageSender;

@Builder
public class TaskCallbackImpl implements TaskCallBack {

    protected final Logger logger =
            LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, TaskCallbackImpl.class));

    private final WorkerMessageSender workerMessageSender;

    private final String masterAddress;

    public TaskCallbackImpl(WorkerMessageSender workerMessageSender, String masterAddress) {
        this.workerMessageSender = workerMessageSender;
        this.masterAddress = masterAddress;
    }

    @Override
    public void updateRemoteApplicationInfo(int taskInstanceId, ApplicationInfo applicationInfo) {
        TaskExecutionContext taskExecutionContext =
                TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId);
        if (taskExecutionContext == null) {
            logger.error("task execution context is empty, taskInstanceId: {}, applicationInfo:{}", taskInstanceId,
                    applicationInfo);
            return;
        }

        logger.info("send remote application info {}", applicationInfo);
        taskExecutionContext.setAppIds(applicationInfo.getAppIds());
        workerMessageSender.sendMessageWithRetry(taskExecutionContext, masterAddress, CommandType.TASK_EXECUTE_RUNNING);
    }
}
