/*
 * Copyright (C) 2017 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.sabot.rpc.user;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.codahale.metrics.Gauge;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.rpc.EventLoopCloseable;
import com.dremio.exec.rpc.TransportCheck;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.metrics.Metrics;
import com.dremio.service.Service;
import com.google.common.base.Preconditions;

import io.netty.channel.EventLoopGroup;

public class UserServer implements Service {

  private final BootStrapContext context;
  private final Provider<UserWorker> worker;
  private final boolean allowPortHunting;
  private final Provider<SabotContext> dbContext;
  private final InboundImpersonationManager impersonationManager;

  private UserRPCServer server;
  private BufferAllocator allocator;
  private EventLoopCloseable eventLoopCloseable;

  private volatile int port = -1;

  public UserServer(BootStrapContext context, Provider<SabotContext> dbContext, Provider<UserWorker> worker, InboundImpersonationManager impersonationManager, boolean allowPortHunting) {
    this.context = context;
    this.worker = worker;
    this.allowPortHunting = allowPortHunting;
    this.dbContext = dbContext;
    this.impersonationManager = impersonationManager;
  }

  public int getPort() {
    Preconditions.checkArgument(port != -1, "Server port cannot be requested before ClientServer is started.");
    return port;
  }

  @Override
  public void start() throws Exception {
    final EventLoopGroup eventLoopGroup = TransportCheck
        .createEventLoopGroup(context.getConfig().getInt(ExecConstants.USER_SERVER_RPC_THREADS), "UserServer-");

    this.eventLoopCloseable = new EventLoopCloseable(eventLoopGroup);
    this.allocator = context.getAllocator().newChildAllocator(
        "rpc:user",
        context.getConfig().getLong("dremio.exec.rpc.user.server.memory.reservation"),
        context.getConfig().getLong("dremio.exec.rpc.user.server.memory.maximum"));

    this.server = new UserRPCServer(context, dbContext, worker, allocator, eventLoopGroup, impersonationManager);

    Metrics.registerGauge("rpc.user.current", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return allocator.getAllocatedMemory();
      }
    });
    Metrics.registerGauge("rpc.user.peak", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return allocator.getPeakMemoryAllocation();
      }
    });
    int initialPort = context.getConfig().getInt(ExecConstants.INITIAL_USER_PORT);
    if(allowPortHunting){
      initialPort += 333;
    }

    port = server.bind(initialPort, allowPortHunting);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(server, eventLoopCloseable, allocator);
  }

}
