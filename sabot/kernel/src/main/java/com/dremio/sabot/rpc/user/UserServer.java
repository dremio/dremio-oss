/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.authenticator.Authenticator;
import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.EventLoopCloseable;
import com.dremio.exec.rpc.TransportCheck;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionValidatorListing;
import com.dremio.service.Service;
import com.dremio.service.users.UserService;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.base.Preconditions;

import io.netty.channel.EventLoopGroup;
import io.opentracing.Tracer;

/**
 * Manages the lifecycle of a {@link UserRPCServer}, which allows for communication with non-web clients.
 */
public class UserServer implements Service {

  private final DremioConfig config;
  private final Provider<ExecutorService> executorService;
  private final Provider<BufferAllocator> bufferAllocator;
  private final Provider<? extends Authenticator> authenticatorProvider;
  private final Provider<UserService> userServiceProvider;
  private final Provider<NodeEndpoint> nodeEndpointProvider;
  private final Provider<UserWorker> worker;
  private final boolean allowPortHunting;
  protected final Tracer tracer;
  private final Provider<OptionValidatorListing> optionValidatorProvider;

  private EventLoopCloseable eventLoopCloseable;
  private BufferAllocator allocator;
  private UserRPCServer server;

  private volatile int port = -1;

  public UserServer(
    DremioConfig config,
    Provider<ExecutorService> executorService,
    Provider<BufferAllocator> bufferAllocator,
    Provider<? extends Authenticator> authenticatorProvider,
    Provider<UserService> userServiceProvider,
    Provider<NodeEndpoint> nodeEndpointProvider,
    Provider<UserWorker> worker,
    boolean allowPortHunting,
    Tracer tracer,
    Provider<OptionValidatorListing> optionValidatorProvider
  ) {
    this.config = config;
    this.executorService = executorService;
    this.bufferAllocator = bufferAllocator;
    this.authenticatorProvider = authenticatorProvider;
    this.userServiceProvider = userServiceProvider;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.worker = worker;
    this.allowPortHunting = allowPortHunting;
    this.tracer = tracer;
    this.optionValidatorProvider = optionValidatorProvider;
  }

  public int getPort() {
    Preconditions.checkArgument(port != -1, "Server port cannot be requested before UserRPCServer is started.");
    return port;
  }

  @Override
  public void start() throws Exception {
    final SabotConfig sabotConfig = config.getSabotConfig();
    allocator = bufferAllocator.get()
        .newChildAllocator(
            "rpc:user",
            sabotConfig.getLong("dremio.exec.rpc.user.server.memory.reservation"),
          sabotConfig.getLong("dremio.exec.rpc.user.server.memory.maximum"));

    final EventLoopGroup eventLoopGroup = TransportCheck
        .createEventLoopGroup(sabotConfig.getInt(ExecConstants.USER_SERVER_RPC_THREADS), "UserServer-");
    eventLoopCloseable = new EventLoopCloseable(eventLoopGroup);

    server = newUserRPCServer(eventLoopGroup);

    Metrics.newGauge("rpc.user.current", allocator::getAllocatedMemory);
    Metrics.newGauge("rpc.user.peak", allocator::getPeakMemoryAllocation);
    int initialPort = sabotConfig.getInt(DremioClient.INITIAL_USER_PORT);
    if(allowPortHunting){
      initialPort += 333;
    }

    port = server.bind(initialPort, allowPortHunting);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(server, eventLoopCloseable, allocator);
  }

  protected UserRPCServer newUserRPCServer(EventLoopGroup eventLoopGroup) throws Exception {
    final SabotConfig sabotConfig = config.getSabotConfig();

    return new UserRPCServer(
        UserRpcConfig.getMapping(sabotConfig, executorService.get(), Optional.empty()),
        authenticatorProvider,
        userServiceProvider,
        nodeEndpointProvider,
        getWorker(),
        getAllocator(),
        eventLoopGroup,
        null,
        tracer,
        optionValidatorProvider.get());
  }

  protected Provider<? extends Authenticator> getAuthenticatorProvider() {
    return authenticatorProvider;
  }

  protected Provider<UserService> getUserServiceProvider() {
    return userServiceProvider;
  }

  protected Provider<NodeEndpoint> getNodeEndpointProvider() {
    return nodeEndpointProvider;
  }

  protected Provider<UserWorker> getWorker() {
    return worker;
  }

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  protected DremioConfig getConfig() {
    return config;
  }

  protected Provider<ExecutorService> getExecutorService() {
    return executorService;
  }

  protected Tracer getTracer() {
    return tracer;
  }

  protected Provider<OptionValidatorListing> getOptionValidatorProvider() {
    return optionValidatorProvider;
  }

}
