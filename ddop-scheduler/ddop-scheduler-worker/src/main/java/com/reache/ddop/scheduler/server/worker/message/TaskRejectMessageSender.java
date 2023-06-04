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
import com.reache.ddop.scheduler.remote.command.TaskRejectCommand;
import com.reache.ddop.scheduler.remote.exceptions.RemotingException;
import com.reache.ddop.scheduler.remote.utils.Host;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.worker.config.WorkerConfig;
import com.reache.ddop.scheduler.server.worker.rpc.WorkerRpcClient;

@Component
public class TaskRejectMessageSender implements MessageSender<TaskRejectCommand> {

    @Autowired
    private WorkerRpcClient workerRpcClient;

    @Autowired
    private WorkerConfig workerConfig;

    @Override
    public void sendMessage(TaskRejectCommand message) throws RemotingException {
        workerRpcClient.send(Host.of(message.getMessageReceiverAddress()), message.convert2Command());
    }

    public TaskRejectCommand buildMessage(TaskExecutionContext taskExecutionContext, String masterAddress) {
        TaskRejectCommand taskRejectMessage = new TaskRejectCommand(workerConfig.getWorkerAddress(),
                                                                    masterAddress,
                                                                    System.currentTimeMillis());
        taskRejectMessage.setTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        taskRejectMessage.setProcessInstanceId(taskExecutionContext.getProcessInstanceId());
        taskRejectMessage.setHost(taskExecutionContext.getHost());
        return taskRejectMessage;
    }

    @Override
    public CommandType getMessageType() {
        return CommandType.TASK_REJECT;
    }
}
