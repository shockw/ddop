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

package com.reache.ddop.scheduler.server.master.event;

import com.reache.ddop.scheduler.common.enums.StateEventType;

import com.google.auto.service.AutoService;
import com.reache.ddop.scheduler.server.master.metrics.ProcessInstanceMetrics;
import com.reache.ddop.scheduler.server.master.runner.WorkflowExecuteRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(StateEventHandler.class)
public class WorkflowTimeoutStateEventHandler implements StateEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowTimeoutStateEventHandler.class);

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent) {
        logger.info("Handle workflow instance timeout event");
        ProcessInstanceMetrics.incProcessInstanceByState("timeout");
        workflowExecuteRunnable.processTimeout();
        return true;
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.PROCESS_TIMEOUT;
    }
}
