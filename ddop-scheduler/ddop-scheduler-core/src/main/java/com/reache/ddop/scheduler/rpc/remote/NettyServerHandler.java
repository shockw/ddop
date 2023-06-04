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

package com.reache.ddop.scheduler.rpc.remote;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reache.ddop.scheduler.rpc.common.RpcRequest;
import com.reache.ddop.scheduler.rpc.common.RpcResponse;
import com.reache.ddop.scheduler.rpc.common.ThreadPoolManager;
import com.reache.ddop.scheduler.rpc.config.ServiceBean;
import com.reache.ddop.scheduler.rpc.protocol.EventType;
import com.reache.ddop.scheduler.rpc.protocol.RpcProtocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * NettyServerHandler
 */
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    private static final ThreadPoolManager threadPoolManager = ThreadPoolManager.INSTANCE;

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("channel close");
        ctx.channel().close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("client connect success !" + ctx.channel().remoteAddress());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RpcProtocol<RpcRequest> rpcProtocol = (RpcProtocol<RpcRequest>) msg;
        if (rpcProtocol.getMsgHeader().getEventType() == EventType.HEARTBEAT.getType()) {
            logger.info("heart beat");
            return;
        }
        threadPoolManager.addExecuteTask(() -> readHandler(ctx, rpcProtocol));
    }

    private void readHandler(ChannelHandlerContext ctx, RpcProtocol protocol) {
        RpcRequest req = (RpcRequest) protocol.getBody();
        RpcResponse response = new RpcResponse();

        response.setStatus((byte) 0);

        String classname = req.getClassName();

        String methodName = req.getMethodName();

        Class<?>[] parameterTypes = req.getParameterTypes();

        Object[] arguments = req.getParameters();
        Object result = null;
        try {
            Class serviceClass = ServiceBean.getServiceClass(classname);

            Object object = serviceClass.newInstance();

            Method method = serviceClass.getMethod(methodName, parameterTypes);

            result = method.invoke(object, arguments);
        } catch (Exception e) {
            logger.error("netty server execute error,service name :{} method name :{} ", classname + methodName, e);
            response.setStatus((byte) -1);
        }

        response.setResult(result);
        protocol.setBody(response);
        protocol.getMsgHeader().setEventType(EventType.RESPONSE.getType());
        ctx.writeAndFlush(protocol);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            logger.debug("IdleStateEvent triggered, send heartbeat to channel " + ctx.channel());
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("exceptionCaught : {}", cause.getMessage(), cause);
        ctx.channel().close();
    }
}
