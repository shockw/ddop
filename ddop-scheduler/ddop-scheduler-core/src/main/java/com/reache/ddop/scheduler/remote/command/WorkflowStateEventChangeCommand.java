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

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

import com.reache.ddop.scheduler.common.enums.WorkflowExecutionStatus;
import com.reache.ddop.scheduler.common.utils.JSONUtils;

/**
 * db task final result response command
 */
@Data
@NoArgsConstructor
public class WorkflowStateEventChangeCommand implements Serializable {

    private String key;

    private WorkflowExecutionStatus sourceStatus;

    private int sourceProcessInstanceId;

    private int sourceTaskInstanceId;

    private int destProcessInstanceId;

    private int destTaskInstanceId;

    public WorkflowStateEventChangeCommand(int sourceProcessInstanceId,
                                           int sourceTaskInstanceId,
                                           WorkflowExecutionStatus sourceStatus,
                                           int destProcessInstanceId,
                                           int destTaskInstanceId) {
        this.key = String.format("%d-%d-%d-%d",
                sourceProcessInstanceId,
                sourceTaskInstanceId,
                destProcessInstanceId,
                destTaskInstanceId);

        this.sourceStatus = sourceStatus;
        this.sourceProcessInstanceId = sourceProcessInstanceId;
        this.sourceTaskInstanceId = sourceTaskInstanceId;
        this.destProcessInstanceId = destProcessInstanceId;
        this.destTaskInstanceId = destTaskInstanceId;
    }

    /**
     * package response command
     *
     * @return command
     */
    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.STATE_EVENT_REQUEST);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

}
