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

package com.reache.ddop.scheduler.server.master.task;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import com.reache.ddop.scheduler.common.lifecycle.ServerLifeCycleManager;
import com.reache.ddop.scheduler.common.model.BaseHeartBeatTask;
import com.reache.ddop.scheduler.common.model.MasterHeartBeat;
import com.reache.ddop.scheduler.common.utils.JSONUtils;
import com.reache.ddop.scheduler.common.utils.OSUtils;
import com.reache.ddop.scheduler.service.registry.RegistryClient;

import com.reache.ddop.scheduler.server.master.config.MasterConfig;

@Slf4j
public class MasterHeartBeatTask extends BaseHeartBeatTask<MasterHeartBeat> {

    private final MasterConfig masterConfig;

    private final RegistryClient registryClient;

    private final String heartBeatPath;

    private final int processId;

    public MasterHeartBeatTask(@NonNull MasterConfig masterConfig,
                               @NonNull RegistryClient registryClient) {
        super("MasterHeartBeatTask", masterConfig.getHeartbeatInterval().toMillis());
        this.masterConfig = masterConfig;
        this.registryClient = registryClient;
        this.heartBeatPath = masterConfig.getMasterRegistryPath();
        this.processId = OSUtils.getProcessID();
    }

    @Override
    public MasterHeartBeat getHeartBeat() {
        return MasterHeartBeat.builder()
                .startupTime(ServerLifeCycleManager.getServerStartupTime())
                .reportTime(System.currentTimeMillis())
                .cpuUsage(OSUtils.cpuUsage())
                .loadAverage(OSUtils.loadAverage())
                .availablePhysicalMemorySize(OSUtils.availablePhysicalMemorySize())
                .maxCpuloadAvg(masterConfig.getMaxCpuLoadAvg())
                .reservedMemory(masterConfig.getReservedMemory())
                .memoryUsage(OSUtils.memoryUsage())
                .diskAvailable(OSUtils.diskAvailable())
                .processId(processId)
                .build();
    }

    @Override
    public void writeHeartBeat(MasterHeartBeat masterHeartBeat) {
        String masterHeartBeatJson = JSONUtils.toJsonString(masterHeartBeat);
        registryClient.persistEphemeral(heartBeatPath, masterHeartBeatJson);
        log.info("Success write master heartBeatInfo into registry, masterRegistryPath: {}, heartBeatInfo: {}",
                heartBeatPath, masterHeartBeatJson);
    }
}
