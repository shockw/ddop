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

package com.reache.ddop.scheduler.server.worker.processor;

import com.google.common.base.Preconditions;
import com.reache.ddop.scheduler.server.worker.message.MessageRetryRunner;

import io.netty.channel.Channel;
import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.command.TaskExecuteAckCommand;
import com.reache.ddop.scheduler.remote.processor.NettyRequestProcessor;
import com.reache.ddop.scheduler.service.utils.LoggerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * task execute running ack, from master to worker
 */
@Component
public class TaskExecuteResultAckProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskExecuteResultAckProcessor.class);

    @Autowired
    private MessageRetryRunner messageRetryRunner;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_EXECUTE_RESULT_ACK == command.getType(),
                String.format("invalid command type : %s", command.getType()));

        TaskExecuteAckCommand taskExecuteAckMessage = JSONUtils.parseObject(command.getBody(),
                TaskExecuteAckCommand.class);

        if (taskExecuteAckMessage == null) {
            logger.error("task execute response ack command is null");
            return;
        }

        try {
            LoggerUtils.setTaskInstanceIdMDC(taskExecuteAckMessage.getTaskInstanceId());
            logger.info("Receive task execute response ack command : {}", taskExecuteAckMessage);
            if (taskExecuteAckMessage.isSuccess()) {
                messageRetryRunner.removeRetryMessage(taskExecuteAckMessage.getTaskInstanceId(),
                        CommandType.TASK_EXECUTE_RESULT);
                logger.debug("remove REMOTE_CHANNELS, task instance id:{}", taskExecuteAckMessage.getTaskInstanceId());
            } else {
                // master handle worker response error, will still retry
                logger.error("Receive task execute result ack message, the message status is not success, message: {}",
                        taskExecuteAckMessage);
            }
        } finally {
            LoggerUtils.removeTaskInstanceIdMDC();

        }
    }

}
