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

package com.reache.ddop.scheduler.service.cache;

import com.reache.ddop.scheduler.common.enums.CacheType;
import com.reache.ddop.scheduler.common.enums.NodeType;
import com.reache.ddop.scheduler.common.model.Server;
import com.reache.ddop.scheduler.dao.entity.User;
import com.reache.ddop.scheduler.remote.NettyRemotingServer;
import com.reache.ddop.scheduler.remote.command.CacheExpireCommand;
import com.reache.ddop.scheduler.remote.command.Command;
import com.reache.ddop.scheduler.remote.command.CommandType;
import com.reache.ddop.scheduler.remote.config.NettyServerConfig;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.reache.ddop.scheduler.service.cache.impl.CacheNotifyServiceImpl;
import com.reache.ddop.scheduler.service.registry.RegistryClient;

/**
 * tenant cache proxy test
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class CacheNotifyServiceTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CacheNotifyServiceImpl cacheNotifyService;

    @Mock
    private RegistryClient registryClient;

    @Test
    public void testNotifyMaster() {
        User user1 = new User();
        user1.setId(100);
        Command cacheExpireCommand = new CacheExpireCommand(CacheType.USER, "100").convert2Command();

        NettyServerConfig serverConfig = new NettyServerConfig();

        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(serverConfig);
        nettyRemotingServer.registerProcessor(CommandType.CACHE_EXPIRE, (channel, command) -> {
            Assert.assertEquals(cacheExpireCommand, command);
        });
        nettyRemotingServer.start();

        List<Server> serverList = new ArrayList<>();
        Server server = new Server();
        server.setHost("127.0.0.1");
        server.setPort(serverConfig.getListenPort());
        serverList.add(server);

        Mockito.when(registryClient.getServerList(NodeType.MASTER)).thenReturn(serverList);

        cacheNotifyService.notifyMaster(cacheExpireCommand);

        nettyRemotingServer.close();
    }
}
