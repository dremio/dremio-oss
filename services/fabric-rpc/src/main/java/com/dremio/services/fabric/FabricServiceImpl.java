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
package com.dremio.services.fabric;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLException;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.rpc.EventLoopCloseable;
import com.dremio.exec.rpc.RpcCommand;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.TransportCheck;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.ssl.SSLEngineFactory;
import com.google.protobuf.MessageLite;

import io.netty.channel.EventLoopGroup;

/**
 * Fabric service implementation. Manages node-to-node communication.
 */
public class FabricServiceImpl implements FabricService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FabricServiceImpl.class);

  private final FabricMessageHandler handler = new FabricMessageHandler();

  private final String address;
  private final int initialPort;
  private final boolean allowPortHunting;
  private final int threadCount;
  private final BufferAllocator bootstrapAllocator;
  private final long reservationInBytes;
  private final long maxAllocationInBytes;

  private final RpcConfig rpcConfig;

  private BufferAllocator allocator;
  private EventLoopGroup eventLoop;
  private EventLoopCloseable eventLoopCloseable;
  private ConnectionManagerRegistry registry;
  private FabricServer server;

  private volatile int port = -1;

  public FabricServiceImpl(
      String address,
      int initialPort,
      boolean allowPortHunting,
      int threadCount,
      BufferAllocator bootstrapAllocator,
      long reservationInBytes,
      long maxAllocationInBytes,
      int timeoutInSeconds,
      Executor rpcHandleDispatcher
  ) {
    this.address = address;
    this.initialPort = allowPortHunting ? initialPort + 333 : initialPort;
    this.allowPortHunting = allowPortHunting;
    this.threadCount = threadCount;
    this.bootstrapAllocator = bootstrapAllocator;
    this.reservationInBytes = reservationInBytes;
    this.maxAllocationInBytes = maxAllocationInBytes;

    rpcConfig = FabricRpcConfig.getMapping(timeoutInSeconds, rpcHandleDispatcher, Optional.empty());
  }

  @Override
  public FabricRunnerFactory registerProtocol(FabricProtocol protocol) {
    handler.registerProtocol(protocol);
    return new RunnerFactory(protocol);
  }

  @Override
  public FabricRunnerFactory getProtocol(int id) {
    FabricProtocol protocol = handler.getProtocol(id);
    return new RunnerFactory(protocol);
  }

  @Override
  public void start() throws Exception {
    allocator = bootstrapAllocator.newChildAllocator("fabric-allocator", reservationInBytes, maxAllocationInBytes);
    logger.info("fabric service has {} bytes reserved", reservationInBytes);

    eventLoop = TransportCheck.createEventLoopGroup(threadCount, "FABRIC-");
    eventLoopCloseable = new EventLoopCloseable(eventLoop);

    registry = new ConnectionManagerRegistry(getRpcConfig(), eventLoop, allocator, handler, getSSLEngineFactory());

    server = newFabricServer();

    port = server.bind(initialPort, allowPortHunting);

    registry.setIdentity(FabricIdentity.newBuilder()
        .setAddress(address)
        .setPort(port)
        .build());
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(server, registry, eventLoopCloseable, allocator);
  }

  protected Optional<SSLEngineFactory> getSSLEngineFactory() throws SSLException {
    return Optional.empty();
  }

  protected FabricServer newFabricServer() throws Exception {
    return new FabricServer(getAddress(), getHandler(), getRpcConfig(), getAllocator(), getRegistry(), getEventLoop());
  }

  protected FabricMessageHandler getHandler() {
    return handler;
  }

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  protected RpcConfig getRpcConfig() {
    return rpcConfig;
  }

  protected EventLoopGroup getEventLoop() {
    return eventLoop;
  }

  protected ConnectionManagerRegistry getRegistry() {
    return registry;
  }

  private static class CommandRunner implements FabricCommandRunner {

    private FabricProtocol protocol;
    private FabricConnectionManager manager;

    public CommandRunner(FabricProtocol protocol, FabricConnectionManager manager) {
      super();
      this.protocol = protocol;
      this.manager = manager;
    }

    @Override
    public <R extends MessageLite, C extends RpcCommand<R, ProxyConnection>> void runCommand(C cmd) {
      manager.runCommand(new ProxyCommand<>(cmd, protocol));
    }

  }

  private static class ProxyCommand<R extends MessageLite> implements RpcCommand<R, FabricConnection> {

    private final RpcCommand<R, ProxyConnection> proxyCommand;
    private final FabricProtocol protocol;

    public ProxyCommand(RpcCommand<R, ProxyConnection> proxyCommand, FabricProtocol protocol) {
      super();
      this.proxyCommand = proxyCommand;
      this.protocol = protocol;
    }

    @Override
    public void connectionSucceeded(FabricConnection connection) {
      proxyCommand.connectionAvailable(new ProxyConnection(connection, protocol));
    }

    @Override
    public void connectionFailed(com.dremio.exec.rpc.RpcConnectionHandler.FailureType type, Throwable t) {
      proxyCommand.connectionFailed(type, t);
    }

    @Override
    public void connectionAvailable(FabricConnection connection) {
      proxyCommand.connectionAvailable(new ProxyConnection(connection, protocol));
    }

  }

  private class RunnerFactory implements FabricRunnerFactory {
    private final FabricProtocol protocol;

    public RunnerFactory(FabricProtocol protocol) {
      super();
      this.protocol = protocol;
    }

    @Override
    public FabricCommandRunner getCommandRunner(String address, int port) {
      final FabricConnectionManager manager = registry.getConnectionManager(FabricIdentity.newBuilder().setAddress(address).setPort(port).build());
      return new CommandRunner(protocol, manager);
    }
  }

  public static String getAddress(boolean useIP) throws UnknownHostException {
    return useIP ? InetAddress.getLocalHost().getHostAddress() : InetAddress.getLocalHost().getCanonicalHostName();
  }

}

