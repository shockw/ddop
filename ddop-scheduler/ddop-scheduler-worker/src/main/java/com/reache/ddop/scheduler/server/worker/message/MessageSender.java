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
import com.reache.ddop.scheduler.remote.command.BaseCommand;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.exceptions.RemotingException;

public interface MessageSender<T extends BaseCommand> {

    /**
     * Send the message
     *
     * @throws RemotingException Cannot connect to the target host.
     */
    void sendMessage(T message) throws RemotingException;

    /**
     * Build the message from task context and message received address.
     */
    T buildMessage(TaskExecutionContext taskExecutionContext, String messageReceiverAddress);

    /**
     * The message type can be sent by this sender.
     */
    CommandType getMessageType();
}
