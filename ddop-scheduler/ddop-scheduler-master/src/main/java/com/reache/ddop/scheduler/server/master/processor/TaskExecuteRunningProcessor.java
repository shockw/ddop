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

package com.reache.ddop.scheduler.server.master.processor;

import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.command.TaskExecuteRunningCommand;
import com.reache.ddop.scheduler.remote.processor.NettyRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.reache.ddop.scheduler.server.master.processor.queue.TaskEvent;
import com.reache.ddop.scheduler.server.master.processor.queue.TaskEventService;

import io.netty.channel.Channel;

/**
 * task execute running processor
 */
@Component
public class TaskExecuteRunningProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskExecuteRunningProcessor.class);

    @Autowired
    private TaskEventService taskEventService;

    /**
     * task ack process
     *
     * @param channel channel channel
     * @param command command TaskExecuteAckCommand
     */
    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_EXECUTE_RUNNING == command.getType(), String.format("invalid command type : %s", command.getType()));
        TaskExecuteRunningCommand taskExecuteRunningMessage = JSONUtils.parseObject(command.getBody(), TaskExecuteRunningCommand.class);
        logger.info("taskExecuteRunningCommand: {}", taskExecuteRunningMessage);

        TaskEvent taskEvent = TaskEvent.newRunningEvent(taskExecuteRunningMessage,
                                                        channel,
                                                        taskExecuteRunningMessage.getMessageSenderAddress());
        taskEventService.addEvent(taskEvent);
    }

}
