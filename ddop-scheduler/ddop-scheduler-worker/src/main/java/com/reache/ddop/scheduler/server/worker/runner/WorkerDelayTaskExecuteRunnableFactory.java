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

import lombok.NonNull;
import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.service.alert.AlertClientService;
import com.reache.ddop.scheduler.service.storage.StorageOperate;
import com.reache.ddop.scheduler.service.task.TaskPluginManager;

import com.reache.ddop.scheduler.server.worker.config.WorkerConfig;
import com.reache.ddop.scheduler.server.worker.rpc.WorkerMessageSender;

import javax.annotation.Nullable;

public abstract class WorkerDelayTaskExecuteRunnableFactory<T extends WorkerDelayTaskExecuteRunnable> implements WorkerTaskExecuteRunnableFactory<T> {

    protected final @NonNull TaskExecutionContext taskExecutionContext;
    protected final @NonNull WorkerConfig workerConfig;
    protected final @NonNull String workflowMasterAddress;
    protected final @NonNull WorkerMessageSender workerMessageSender;
    protected final @NonNull AlertClientService alertClientService;
    protected final @NonNull TaskPluginManager taskPluginManager;
    protected final @Nullable StorageOperate storageOperate;

    protected WorkerDelayTaskExecuteRunnableFactory(
            @NonNull TaskExecutionContext taskExecutionContext,
            @NonNull WorkerConfig workerConfig,
            @NonNull String workflowMasterAddress,
            @NonNull WorkerMessageSender workerMessageSender,
            @NonNull AlertClientService alertClientService,
            @NonNull TaskPluginManager taskPluginManager,
            @Nullable StorageOperate storageOperate) {
        this.taskExecutionContext = taskExecutionContext;
        this.workerConfig = workerConfig;
        this.workflowMasterAddress = workflowMasterAddress;
        this.workerMessageSender = workerMessageSender;
        this.alertClientService = alertClientService;
        this.taskPluginManager = taskPluginManager;
        this.storageOperate = storageOperate;
    }


    public abstract T createWorkerTaskExecuteRunnable();
}
