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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Test;

import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.ChannelClosedException;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RemoteConnection;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * Test fabric behaviors that aren't available at the protocol builder level.
 */
public class TestFabric extends BaseTestFabric {

  protected FabricService getFabric() {
    return fabric;
  }

  @Test
  public void firstDisconnectRecovery() throws Exception {
    CountDownLatch closeLatch = new CountDownLatch(1);
    FabricRunnerFactory factory = getFabric().registerProtocol(new Protocol(closeLatch));
    FabricCommandRunner runner = factory.getCommandRunner(getFabric().getAddress(), getFabric().getPort());


    // send a message, establishing the connection.
    {
      SimpleMessage m = new SimpleMessage(1);
      runner.runCommand(m);
      DremioFutures.getChecked(m.getFuture(), RpcException.class, 1000, TimeUnit.MILLISECONDS, RpcException::mapException);
    }

    closeLatch.countDown();
    // wait for the local connection to be closed.
    Thread.sleep(1000);


    // ensure we can send message again.
    {
      SimpleMessage m = new SimpleMessage(1);
      runner.runCommand(m);
      DremioFutures.getChecked(m.getFuture(), RpcException.class, 1000, TimeUnit.MILLISECONDS, RpcException::mapException);
    }


  }

  @Test(expected=ChannelClosedException.class)
  public void failureOnDisconnection() throws Exception {
    FabricRunnerFactory factory = getFabric().registerProtocol(new Protocol(null));
    FabricCommandRunner runner = factory.getCommandRunner(getFabric().getAddress(), getFabric().getPort());
    SimpleMessage m = new SimpleMessage(2);
    runner.runCommand(m);
    DremioFutures.getChecked(m.getFuture(), RpcException.class, 1000, TimeUnit.MILLISECONDS, RpcException::mapException);
  }

  private class SimpleMessage extends FutureBitCommand<NodeEndpoint, ProxyConnection> {

    private final int type;

    public SimpleMessage(int type) {
      super();
      this.type = type;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<NodeEndpoint> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, new FakeEnum(type), expectedQ, NodeEndpoint.class);
    }

  }

  private class Protocol implements FabricProtocol {

    private final CountDownLatch closeLatch;
    private final CountDownLatch depleteResponseQueue;

    public Protocol(CountDownLatch closeLatch) {
      this(closeLatch, null);
    }

    public Protocol(CountDownLatch closeLatch, CountDownLatch depleteResponseQueue) {
      super();
      this.closeLatch = closeLatch;
      this.depleteResponseQueue = depleteResponseQueue;
    }


    @Override
    public int getProtocolId() {
      return 2;
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public RpcConfig getConfig() {
      return RpcConfig.newBuilder().name("test1").timeout(0)
          .add(new FakeEnum(1), QueryId.class, new FakeEnum(1), NodeEndpoint.class)
          .add(new FakeEnum(2), QueryId.class, new FakeEnum(2), NodeEndpoint.class)
          .build();
    }

    @Override
    public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
      switch(rpcType){
      case 1:
      case 2:
        return NodeEndpoint.getDefaultInstance();

      default:
        throw new UnsupportedOperationException();

      }

    }

    @Override
    public void handle(final PhysicalConnection connection, int rpcType, ByteString pBody, ByteBuf dBody,
                       ResponseSender sender) throws RpcException {

      switch(rpcType){
      case 1:
        sender.send(new Response(new FakeEnum(1), expectedD));
        new Thread(){

          @Override
          public void run() {
            try {
              closeLatch.await();
              ((RemoteConnection)connection).getChannel().close();
            } catch (InterruptedException e) {
            }
          }

        }.start();

        return;

      case 2:
        ((RemoteConnection)connection).getChannel().close();
        return;

      default:
        throw new UnsupportedOperationException();
      }


    }

  }
}
