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
package com.dremio.services.fabric.simple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

import io.netty.buffer.NettyArrowBuf;

/**
 * Implementation class the provides endpoints using a FabricRunnerFactory,
 * abstracting away the asynchronous nature of the Fabric protocol system.
 *
 * @param <REQUEST>
 * @param <RESPONSE>
 */
class EndpointCreator<REQUEST extends MessageLite, RESPONSE extends MessageLite>
    implements SendEndpointCreator<REQUEST, RESPONSE> {

  private final FabricRunnerFactory factory;
  private final EnumLite num;
  private final Class<RESPONSE> responseClass;
  private final long timeout;

  public EndpointCreator(FabricRunnerFactory factory, EnumLite num, Class<RESPONSE> responseClass, long timeout) {
    super();
    this.factory = factory;
    this.num = num;
    this.responseClass = responseClass;
    this.timeout = timeout;
  }

  @Override
  public SendEndpoint<REQUEST, RESPONSE> getEndpoint(String address, int port) {
    return new RequestSender(factory.getCommandRunner(address, port));
  }

  /**
   * Class that is used to send requests by api consumers.
   */
  private class RequestSender implements SendEndpoint<REQUEST, RESPONSE> {

    private final FabricCommandRunner runner;

    public RequestSender(FabricCommandRunner runner) {
      super();
      this.runner = runner;
    }

    @Override
    public ReceivedResponseMessage<RESPONSE> send(REQUEST message, ArrowBuf... bufs) throws RpcException {
      Command c = new Command(message, bufs);
      runner.runCommand(c);
      RpcFuture<RESPONSE> future = c.getFuture();
      final RESPONSE response;

      if (timeout > 0) {
        try {
          response = DremioFutures.getChecked(future, RpcException.class, timeout, TimeUnit.MILLISECONDS, RpcException::mapException);
        } catch (TimeoutException ex) {
          throw new RpcException(ex);
        }
      } else {
        response = DremioFutures.getChecked(future, RpcException.class, RpcException::mapException);
      }
      final ArrowBuf body = future.getBuffer() != null ? ((NettyArrowBuf) future.getBuffer())
        .arrowBuf() : null;

      return new ReceivedResponseMessage<>(response, body);
    }

    /**
     * A nested command class that actually executes the Rpc operation.
     */
    private class Command extends FutureBitCommand<RESPONSE, ProxyConnection> {
      private final REQUEST req;
      private final NettyArrowBuf[] bufs;

      public Command(REQUEST req, ArrowBuf[] bufs) {
        super();
        this.req = req;
        List<NettyArrowBuf> buffers = Arrays.stream(bufs).map(buf -> NettyArrowBuf.unwrapBuffer(buf)).collect(Collectors.toList());
        this.bufs = new NettyArrowBuf[buffers.size()];
        int i = 0;
        for (NettyArrowBuf arrowBuf : buffers) {
          this.bufs[i] = arrowBuf;
          i++;
        }
      }

      @Override
      public void doRpcCall(RpcOutcomeListener<RESPONSE> outcomeListener, ProxyConnection connection) {
        connection.send(outcomeListener, num, req, responseClass, bufs);
      }

    }
  }

}
