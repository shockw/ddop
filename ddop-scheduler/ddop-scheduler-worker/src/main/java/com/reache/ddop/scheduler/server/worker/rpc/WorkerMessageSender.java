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

package com.reache.ddop.scheduler.server.worker.rpc;

import com.reache.ddop.scheduler.plugin.task.api.TaskExecutionContext;
import com.reache.ddop.scheduler.remote.command.BaseCommand;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.exceptions.RemotingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.worker.message.MessageRetryRunner;
import com.reache.ddop.scheduler.server.worker.message.MessageSender;

import lombok.NonNull;

@Component
public class WorkerMessageSender {

    private final Logger logger = LoggerFactory.getLogger(WorkerMessageSender.class);

    @Autowired
    private MessageRetryRunner messageRetryRunner;

    @Autowired
    private List<MessageSender> messageSenders;

    private Map<CommandType, MessageSender> messageSenderMap = new HashMap<>();

    @PostConstruct
    public void init() {
        messageSenders.forEach(messageSender -> messageSenderMap.put(messageSender.getMessageType(),
                messageSender));
    }

    // todo: use message rather than context
    public void sendMessageWithRetry(@NonNull TaskExecutionContext taskExecutionContext,
                                     @NonNull String messageReceiverAddress,
                                     @NonNull CommandType messageType) {
        MessageSender messageSender = messageSenderMap.get(messageType);
        if (messageSender == null) {
            throw new IllegalArgumentException("The messageType is invalidated, messageType: " + messageType);
        }
        BaseCommand baseCommand = messageSender.buildMessage(taskExecutionContext, messageReceiverAddress);
        try {
            messageRetryRunner.addRetryMessage(taskExecutionContext.getTaskInstanceId(), messageType, baseCommand);
            messageSender.sendMessage(baseCommand);
        } catch (RemotingException e) {
            logger.error("Send message error, messageType: {}, message: {}", messageType, baseCommand);
        }
    }

    public void sendMessage(@NonNull TaskExecutionContext taskExecutionContext,
                            @NonNull String messageReceiverAddress,
                            @NonNull CommandType messageType) {
        MessageSender messageSender = messageSenderMap.get(messageType);
        if (messageSender == null) {
            throw new IllegalArgumentException("The messageType is invalidated, messageType: " + messageType);
        }
        BaseCommand baseCommand = messageSender.buildMessage(taskExecutionContext, messageReceiverAddress);
        try {
            messageSender.sendMessage(baseCommand);
        } catch (RemotingException e) {
            logger.error("Send message error, messageType: {}, message: {}", messageType, baseCommand);
        }
    }

}
