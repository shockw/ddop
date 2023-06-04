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

import com.reache.ddop.scheduler.remote.NettyRemotingClient;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.config.NettyClientConfig;
import com.reache.ddop.scheduler.remote.exceptions.RemotingException;
import com.reache.ddop.scheduler.remote.utils.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reache.ddop.scheduler.server.worker.processor.TaskExecuteResultAckProcessor;
import com.reache.ddop.scheduler.server.worker.processor.TaskExecuteRunningAckProcessor;
import com.reache.ddop.scheduler.server.worker.processor.TaskRejectAckProcessor;

/**
 * This rpc client is only used to send message, will not receive message, all response message should send to {@link WorkerRpcServer}.
 */
@Component
public class WorkerRpcClient implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(WorkerRpcClient.class);

    @Autowired
    private TaskExecuteRunningAckProcessor taskExecuteRunningAckProcessor;

    @Autowired
    private TaskExecuteResultAckProcessor taskExecuteResultAckProcessor;

    @Autowired
    private TaskRejectAckProcessor taskRejectAckProcessor;

    private NettyRemotingClient nettyRemotingClient;

    public void start() {
        logger.info("Worker rpc client starting");
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        this.nettyRemotingClient = new NettyRemotingClient(nettyClientConfig);
        // we only use the client to handle the ack message, we can optimize this, send ack to the nettyServer.
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_EXECUTE_RUNNING_ACK,
                                                   taskExecuteRunningAckProcessor);
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_EXECUTE_RESULT_ACK, taskExecuteResultAckProcessor);
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_REJECT_ACK, taskRejectAckProcessor);
        logger.info("Worker rpc client started");
    }

    public void send(Host host, Command command) throws RemotingException {
        nettyRemotingClient.send(host, command);
    }

    public void close() {
        logger.info("Worker rpc client closing");
        nettyRemotingClient.close();
        logger.info("Worker rpc client closed");
    }
}
