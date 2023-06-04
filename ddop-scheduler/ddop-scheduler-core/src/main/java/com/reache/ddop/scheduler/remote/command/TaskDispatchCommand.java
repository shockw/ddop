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

package com.reache.ddop.scheduler.remote.command;

import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;


/**
 * The task dispatch message, means dispatch a task to worker.
 */
@Data
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class TaskDispatchCommand extends BaseCommand {

    private static final long serialVersionUID = -1L;

    private TaskExecutionContext taskExecutionContext;

    public TaskDispatchCommand(TaskExecutionContext taskExecutionContext,
                               String messageSenderAddress,
                               String messageReceiverAddress,
                               long messageSendTime) {
        super(messageSenderAddress, messageReceiverAddress, messageSendTime);
        this.taskExecutionContext = taskExecutionContext;
    }

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.TASK_DISPATCH_REQUEST);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

}