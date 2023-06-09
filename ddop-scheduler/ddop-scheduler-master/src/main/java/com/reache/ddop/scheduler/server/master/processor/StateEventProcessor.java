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

import com.google.common.base.Preconditions;
import com.reache.ddop.scheduler.server.master.event.StateEvent;
import com.reache.ddop.scheduler.server.master.event.TaskStateEvent;
import com.reache.ddop.scheduler.server.master.event.WorkflowStateEvent;
import com.reache.ddop.scheduler.server.master.processor.queue.StateEventResponseService;

import io.netty.channel.Channel;
import com.reache.ddop.scheduler.common.enums.StateEventType;
import com.reache.ddop.scheduler.common.enums.WorkflowExecutionStatus;
import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.command.WorkflowStateEventChangeCommand;
import com.reache.ddop.scheduler.remote.processor.NettyRequestProcessor;
import com.reache.ddop.scheduler.service.utils.LoggerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * handle state event received from master/api
 */
@Component
public class StateEventProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(StateEventProcessor.class);

    @Autowired
    private StateEventResponseService stateEventResponseService;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.STATE_EVENT_REQUEST == command.getType(),
                String.format("invalid command type: %s", command.getType()));

        WorkflowStateEventChangeCommand workflowStateEventChangeCommand =
                JSONUtils.parseObject(command.getBody(), WorkflowStateEventChangeCommand.class);
        StateEvent stateEvent;
        if (workflowStateEventChangeCommand.getDestTaskInstanceId() == 0) {
            stateEvent = createWorkflowStateEvent(workflowStateEventChangeCommand);
        } else {
            stateEvent = createTaskStateEvent(workflowStateEventChangeCommand);
        }

        try {
            LoggerUtils.setWorkflowAndTaskInstanceIDMDC(stateEvent.getProcessInstanceId(),
                    stateEvent.getTaskInstanceId());

            logger.info("Received state change command, event: {}", stateEvent);
            stateEventResponseService.addStateChangeEvent(stateEvent);
        } finally {
            LoggerUtils.removeWorkflowAndTaskInstanceIdMDC();
        }

    }

    private TaskStateEvent createTaskStateEvent(WorkflowStateEventChangeCommand workflowStateEventChangeCommand) {
        return TaskStateEvent.builder()
                .processInstanceId(workflowStateEventChangeCommand.getDestProcessInstanceId())
                .taskInstanceId(workflowStateEventChangeCommand.getDestTaskInstanceId())
                .type(StateEventType.TASK_STATE_CHANGE)
                .key(workflowStateEventChangeCommand.getKey())
                .build();
    }

    private WorkflowStateEvent createWorkflowStateEvent(WorkflowStateEventChangeCommand workflowStateEventChangeCommand) {
        WorkflowExecutionStatus workflowExecutionStatus = workflowStateEventChangeCommand.getSourceStatus();
        if (workflowStateEventChangeCommand.getSourceProcessInstanceId() != workflowStateEventChangeCommand
                .getDestProcessInstanceId()) {
            workflowExecutionStatus = WorkflowExecutionStatus.RUNNING_EXECUTION;
        }
        return WorkflowStateEvent.builder()
                .processInstanceId(workflowStateEventChangeCommand.getDestProcessInstanceId())
                .type(StateEventType.PROCESS_STATE_CHANGE)
                .status(workflowExecutionStatus)
                .key(workflowStateEventChangeCommand.getKey())
                .build();
    }

}
