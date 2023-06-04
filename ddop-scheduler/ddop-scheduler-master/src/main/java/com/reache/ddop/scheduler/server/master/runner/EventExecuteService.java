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

package com.reache.ddop.scheduler.server.master.runner;

import com.reache.ddop.scheduler.common.constants.Constants;
import com.reache.ddop.scheduler.common.lifecycle.ServerLifeCycleManager;
import com.reache.ddop.scheduler.common.thread.BaseDaemonThread;
import com.reache.ddop.scheduler.service.utils.LoggerUtils;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.cache.StreamTaskInstanceExecCacheManager;

@Service
public class EventExecuteService extends BaseDaemonThread {

    private static final Logger logger = LoggerFactory.getLogger(EventExecuteService.class);

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private StreamTaskInstanceExecCacheManager streamTaskInstanceExecCacheManager;

    @Autowired
    private WorkflowExecuteThreadPool workflowExecuteThreadPool;

    @Autowired
    private StreamTaskExecuteThreadPool streamTaskExecuteThreadPool;

    protected EventExecuteService() {
        super("EventServiceStarted");
    }

    @Override
    public synchronized void start() {
        logger.info("Master Event execute service starting");
        super.start();
        logger.info("Master Event execute service started");
    }

    @Override
    public void run() {
        while (!ServerLifeCycleManager.isStopped()) {
            try {
                workflowEventHandler();
                streamTaskEventHandler();
                TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME_MILLIS_SHORT);
            } catch (InterruptedException interruptedException) {
                logger.warn("Master event service interrupted, will exit this loop", interruptedException);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Master event execute service error", e);
            }
        }
    }

    private void workflowEventHandler() {
        for (WorkflowExecuteRunnable workflowExecuteThread : this.processInstanceExecCacheManager.getAll()) {
            try {
                LoggerUtils.setWorkflowInstanceIdMDC(workflowExecuteThread.getProcessInstance().getId());
                workflowExecuteThreadPool.executeEvent(workflowExecuteThread);

            } finally {
                LoggerUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    private void streamTaskEventHandler() {
        for (StreamTaskExecuteRunnable streamTaskExecuteRunnable : streamTaskInstanceExecCacheManager.getAll()) {
            try {
                LoggerUtils.setTaskInstanceIdMDC(streamTaskExecuteRunnable.getTaskInstance().getId());
                streamTaskExecuteThreadPool.executeEvent(streamTaskExecuteRunnable);

            } finally {
                LoggerUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }
}
