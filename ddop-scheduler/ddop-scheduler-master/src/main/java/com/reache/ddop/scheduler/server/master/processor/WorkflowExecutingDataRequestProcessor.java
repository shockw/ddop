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
import com.reache.ddop.scheduler.remote.command.WorkflowExecutingDataRequestCommand;
import com.reache.ddop.scheduler.remote.command.WorkflowExecutingDataResponseCommand;
import com.reache.ddop.scheduler.remote.dto.WorkflowExecuteDto;
import com.reache.ddop.scheduler.remote.processor.NettyRequestProcessor;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.reache.ddop.scheduler.server.master.service.ExecutingService;

import io.netty.channel.Channel;

/**
 * workflow executing data process from api/master
 */
@Component
public class WorkflowExecutingDataRequestProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(WorkflowExecutingDataRequestProcessor.class);

    @Autowired
    private ExecutingService executingService;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.WORKFLOW_EXECUTING_DATA_REQUEST == command.getType(), String.format("invalid command type: %s", command.getType()));

        WorkflowExecutingDataRequestCommand requestCommand = JSONUtils.parseObject(command.getBody(), WorkflowExecutingDataRequestCommand.class);

        logger.info("received command, processInstanceId:{}", requestCommand.getProcessInstanceId());

        Optional<WorkflowExecuteDto> workflowExecuteDtoOptional = executingService.queryWorkflowExecutingData(requestCommand.getProcessInstanceId());

        WorkflowExecutingDataResponseCommand responseCommand = new WorkflowExecutingDataResponseCommand();
        workflowExecuteDtoOptional.ifPresent(responseCommand::setWorkflowExecuteDto);
        channel.writeAndFlush(responseCommand.convert2Command(command.getOpaque()));
    }
}
