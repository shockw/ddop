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

package com.reache.ddop.scheduler.server.worker.message;

import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.command.TaskExecuteResultCommand;
import com.reache.ddop.scheduler.remote.exceptions.RemotingException;
import com.reache.ddop.scheduler.remote.utils.Host;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.worker.config.WorkerConfig;
import com.reache.ddop.scheduler.server.worker.rpc.WorkerRpcClient;

@Component
public class TaskExecuteResultMessageSender implements MessageSender<TaskExecuteResultCommand> {

    @Autowired
    private WorkerConfig workerConfig;

    @Autowired
    private WorkerRpcClient workerRpcClient;

    @Override
    public void sendMessage(TaskExecuteResultCommand message) throws RemotingException {
        workerRpcClient.send(Host.of(message.getMessageReceiverAddress()), message.convert2Command());
    }

    public TaskExecuteResultCommand buildMessage(TaskExecutionContext taskExecutionContext,
                                                 String messageReceiverAddress) {
        TaskExecuteResultCommand taskExecuteResultMessage
            = new TaskExecuteResultCommand(workerConfig.getWorkerAddress(),
                                           messageReceiverAddress,
                                           System.currentTimeMillis());
        taskExecuteResultMessage.setProcessInstanceId(taskExecutionContext.getProcessInstanceId());
        taskExecuteResultMessage.setTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        taskExecuteResultMessage.setStatus(taskExecutionContext.getCurrentExecutionStatus().getCode());
        taskExecuteResultMessage.setLogPath(taskExecutionContext.getLogPath());
        taskExecuteResultMessage.setExecutePath(taskExecutionContext.getExecutePath());
        taskExecuteResultMessage.setAppIds(taskExecutionContext.getAppIds());
        taskExecuteResultMessage.setProcessId(taskExecutionContext.getProcessId());
        taskExecuteResultMessage.setHost(taskExecutionContext.getHost());
        taskExecuteResultMessage.setStartTime(taskExecutionContext.getStartTime());
        taskExecuteResultMessage.setEndTime(taskExecutionContext.getEndTime());
        taskExecuteResultMessage.setVarPool(taskExecutionContext.getVarPool());
        taskExecuteResultMessage.setExecutePath(taskExecutionContext.getExecutePath());
        return taskExecuteResultMessage;
    }

    @Override
    public CommandType getMessageType() {
        return CommandType.TASK_EXECUTE_RESULT;
    }
}
