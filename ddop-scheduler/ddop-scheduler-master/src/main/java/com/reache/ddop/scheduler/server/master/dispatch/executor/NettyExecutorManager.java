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

package com.reache.ddop.scheduler.server.master.dispatch.executor;

import com.reache.ddop.scheduler.common.constants.Constants;
import com.reache.ddop.scheduler.common.thread.ThreadUtils;
import com.reache.ddop.scheduler.remote.NettyRemotingClient;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.config.NettyClientConfig;
import com.reache.ddop.scheduler.remote.utils.Host;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.reache.ddop.scheduler.server.master.dispatch.context.ExecutionContext;
import com.reache.ddop.scheduler.server.master.dispatch.enums.ExecutorType;
import com.reache.ddop.scheduler.server.master.dispatch.exceptions.ExecuteException;
import com.reache.ddop.scheduler.server.master.processor.TaskExecuteResponseProcessor;
import com.reache.ddop.scheduler.server.master.processor.TaskExecuteRunningProcessor;
import com.reache.ddop.scheduler.server.master.processor.TaskKillResponseProcessor;
import com.reache.ddop.scheduler.server.master.processor.TaskRecallProcessor;
import com.reache.ddop.scheduler.server.master.registry.ServerNodeManager;

/**
 * netty executor manager
 */
@Service
public class NettyExecutorManager extends AbstractExecutorManager<Boolean> {

    private final Logger logger = LoggerFactory.getLogger(NettyExecutorManager.class);

    /**
     * server node manager
     */
    @Autowired
    private ServerNodeManager serverNodeManager;

    @Autowired
    private TaskKillResponseProcessor taskKillResponseProcessor;

    @Autowired
    private TaskRecallProcessor taskRecallProcessor;

    /**
     * netty remote client
     */
    private final NettyRemotingClient nettyRemotingClient;

    /**
     * constructor
     */
    public NettyExecutorManager() {
        final NettyClientConfig clientConfig = new NettyClientConfig();
        this.nettyRemotingClient = new NettyRemotingClient(clientConfig);
    }

    @PostConstruct
    public void init() {
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_KILL_RESPONSE, taskKillResponseProcessor);
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_REJECT, taskRecallProcessor);
    }

    /**
     * execute logic
     *
     * @param context context
     * @return result
     * @throws ExecuteException if error throws ExecuteException
     */
    @Override
    public Boolean execute(ExecutionContext context) throws ExecuteException {
        // all nodes
        Set<String> allNodes = getAllNodes(context);
        // fail nodes
        Set<String> failNodeSet = new HashSet<>();
        // build command accord executeContext
        Command command = context.getCommand();
        // execute task host
        Host host = context.getHost();
        boolean success = false;
        while (!success) {
            try {
                doExecute(host, command);
                success = true;
                context.setHost(host);
                // We set the host to taskInstance to avoid when the worker down, this taskInstance may not be
                // failovered, due to the taskInstance's host
                // is not belongs to the down worker ISSUE-10842.
                context.getTaskInstance().setHost(host.getAddress());
            } catch (ExecuteException ex) {
                logger.error("Execute command {} error", command, ex);
                try {
                    failNodeSet.add(host.getAddress());
                    Set<String> tmpAllIps = new HashSet<>(allNodes);
                    Collection<String> remained = CollectionUtils.subtract(tmpAllIps, failNodeSet);
                    if (remained != null && remained.size() > 0) {
                        host = Host.of(remained.iterator().next());
                        logger.error("retry execute command : {} host : {}", command, host);
                    } else {
                        throw new ExecuteException("fail after try all nodes");
                    }
                } catch (Throwable t) {
                    throw new ExecuteException("fail after try all nodes");
                }
            }
        }

        return success;
    }

    @Override
    public void executeDirectly(ExecutionContext context) throws ExecuteException {
        Host host = context.getHost();
        doExecute(host, context.getCommand());
    }

    /**
     * execute logic
     *
     * @param host host
     * @param command command
     * @throws ExecuteException if error throws ExecuteException
     */
    public void doExecute(final Host host, final Command command) throws ExecuteException {
        // retry count，default retry 3
        int retryCount = 3;
        boolean success = false;
        do {
            try {
                nettyRemotingClient.send(host, command);
                success = true;
            } catch (Exception ex) {
                logger.error("Send command to {} error, command: {}", host, command, ex);
                retryCount--;
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            }
        } while (retryCount >= 0 && !success);

        if (!success) {
            throw new ExecuteException(String.format("send command : %s to %s error", command, host));
        }
    }

    /**
     * get all nodes
     *
     * @param context context
     * @return nodes
     */
    private Set<String> getAllNodes(ExecutionContext context) {
        Set<String> nodes = Collections.emptySet();
        /**
         * executor type
         */
        ExecutorType executorType = context.getExecutorType();
        switch (executorType) {
            case WORKER:
                nodes = serverNodeManager.getWorkerGroupNodes(context.getWorkerGroup());
                break;
            case CLIENT:
                break;
            default:
                throw new IllegalArgumentException("invalid executor type : " + executorType);

        }
        return nodes;
    }

}
