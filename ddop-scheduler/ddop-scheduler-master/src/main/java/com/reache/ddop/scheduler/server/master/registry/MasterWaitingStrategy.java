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

package com.reache.ddop.scheduler.server.master.registry;

import com.reache.ddop.scheduler.common.lifecycle.ServerLifeCycleException;
import com.reache.ddop.scheduler.common.lifecycle.ServerLifeCycleManager;
import com.reache.ddop.scheduler.common.lifecycle.ServerStatus;
import com.reache.ddop.scheduler.registry.api.Registry;
import com.reache.ddop.scheduler.registry.api.RegistryException;
import com.reache.ddop.scheduler.registry.api.StrategyType;
import com.reache.ddop.scheduler.service.registry.RegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.reache.ddop.scheduler.server.master.cache.ProcessInstanceExecCacheManager;
import com.reache.ddop.scheduler.server.master.config.MasterConfig;
import com.reache.ddop.scheduler.server.master.event.WorkflowEventQueue;
import com.reache.ddop.scheduler.server.master.rpc.MasterRPCServer;
import com.reache.ddop.scheduler.server.master.runner.StateWheelExecuteThread;

import java.time.Duration;

/**
 * This strategy will change the server status to {@link ServerStatus#WAITING} when disconnect from {@link Registry}.
 */
@Service
@ConditionalOnProperty(prefix = "master.registry-disconnect-strategy", name = "strategy", havingValue = "waiting")
public class MasterWaitingStrategy implements MasterConnectStrategy {

    private final Logger logger = LoggerFactory.getLogger(MasterWaitingStrategy.class);

    @Autowired
    private MasterConfig masterConfig;
    @Autowired
    private RegistryClient registryClient;
    @Autowired
    private MasterRPCServer masterRPCServer;
    @Autowired
    private WorkflowEventQueue workflowEventQueue;
    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;
    @Autowired
    private StateWheelExecuteThread stateWheelExecuteThread;

    @Override
    public void disconnect() {
        try {
            ServerLifeCycleManager.toWaiting();
            clearMasterResource();
            Duration maxWaitingTime = masterConfig.getRegistryDisconnectStrategy().getMaxWaitingTime();
            try {
                logger.info("Master disconnect from registry will try to reconnect in {} s",
                        maxWaitingTime.getSeconds());
                registryClient.connectUntilTimeout(maxWaitingTime);
            } catch (RegistryException ex) {
                throw new ServerLifeCycleException(
                        String.format("Waiting to reconnect to registry in %s failed", maxWaitingTime), ex);
            }
        } catch (ServerLifeCycleException e) {
            String errorMessage = String.format(
                    "Disconnect from registry and change the current status to waiting error, the current server state is %s, will stop the current server",
                    ServerLifeCycleManager.getServerStatus());
            logger.error(errorMessage, e);
            registryClient.getStoppable().stop(errorMessage);
        } catch (RegistryException ex) {
            String errorMessage = "Disconnect from registry and waiting to reconnect failed, will stop the server";
            logger.error(errorMessage, ex);
            registryClient.getStoppable().stop(errorMessage);
        } catch (Exception ex) {
            String errorMessage = "Disconnect from registry and get an unknown exception, will stop the server";
            logger.error(errorMessage, ex);
            registryClient.getStoppable().stop(errorMessage);
        }
    }

    @Override
    public void reconnect() {
        if (ServerLifeCycleManager.isRunning()) {
            logger.info("no need to reconnect, as the current server status is running");
        } else {
            try {
                ServerLifeCycleManager.recoverFromWaiting();
                reStartMasterResource();
                logger.info("Recover from waiting success, the current server status is {}",
                        ServerLifeCycleManager.getServerStatus());
            } catch (Exception e) {
                String errorMessage =
                        String.format(
                                "Recover from waiting failed, the current server status is %s, will stop the server",
                                ServerLifeCycleManager.getServerStatus());
                logger.error(errorMessage, e);
                registryClient.getStoppable().stop(errorMessage);
            }
        }
    }

    @Override
    public StrategyType getStrategyType() {
        return StrategyType.WAITING;
    }

    private void clearMasterResource() {
        // close the worker resource, if close failed should stop the worker server
        masterRPCServer.close();
        logger.warn("Master closed RPC server due to lost registry connection");
        workflowEventQueue.clearWorkflowEventQueue();
        logger.warn("Master clear workflow event queue due to lost registry connection");
        processInstanceExecCacheManager.clearCache();
        logger.warn("Master clear process instance cache due to lost registry connection");
        stateWheelExecuteThread.clearAllTasks();
        logger.warn("Master clear all state wheel task due to lost registry connection");

    }

    private void reStartMasterResource() {
        // reopen the resource, if reopen failed should stop the worker server
        masterRPCServer.start();
        logger.warn("Master restarted RPC server due to reconnect to registry");
    }
}
