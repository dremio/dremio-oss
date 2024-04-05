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
package com.dremio.service.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.function.Supplier;

/**
 * A grpc-level proxy. GrpcProxy itself can be used unmodified to proxy any service for both unary
 * and streaming. It doesn't care what type of messages are being used. The Registry class causes it
 * to be called for any inbound RPC, and uses plain bytes for messages which avoids marshalling
 * messages and the need for Protobuf schema information.
 */
public class GrpcProxy<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
  private final Supplier<ManagedChannel> channelSupplier;

  public GrpcProxy(Supplier<ManagedChannel> channelSupplier) {
    this.channelSupplier = channelSupplier;
  }

  @Override
  public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> serverCall, Metadata headers) {
    ClientCall<ReqT, RespT> clientCall =
        channelSupplier.get().newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
    CallProxy<ReqT, RespT> proxy = new CallProxy<>(serverCall, clientCall);
    clientCall.start(proxy.clientCallListener, headers);
    serverCall.request(1);
    clientCall.request(1);
    return proxy.serverCallListener;
  }

  private static class CallProxy<ReqT, RespT> {
    private final RequestProxy serverCallListener;
    private final ResponseProxy clientCallListener;

    public CallProxy(ServerCall<ReqT, RespT> serverCall, ClientCall<ReqT, RespT> clientCall) {
      serverCallListener = new RequestProxy(clientCall);
      clientCallListener = new ResponseProxy(serverCall);
    }

    private class RequestProxy extends ServerCall.Listener<ReqT> {
      private final ClientCall<ReqT, ?> clientCall;
      // Hold 'this' lock when accessing
      private boolean needToRequest;

      public RequestProxy(ClientCall<ReqT, ?> clientCall) {
        this.clientCall = clientCall;
      }

      @Override
      public void onCancel() {
        clientCall.cancel("Server cancelled", null);
      }

      @Override
      public void onHalfClose() {
        clientCall.halfClose();
      }

      @Override
      public void onMessage(ReqT message) {
        clientCall.sendMessage(message);
        synchronized (this) {
          if (clientCall.isReady()) {
            clientCallListener.serverCall.request(1);
          } else {
            // The outgoing call is not ready for more requests. Stop requesting additional data and
            // wait for it to catch up.
            needToRequest = true;
          }
        }
      }

      @Override
      public void onReady() {
        clientCallListener.onServerReady();
      }

      // Called from ResponseProxy, which is a different thread than the ServerCall.Listener
      // callbacks.
      synchronized void onClientReady() {
        if (needToRequest) {
          clientCallListener.serverCall.request(1);
          needToRequest = false;
        }
      }
    }

    private class ResponseProxy extends ClientCall.Listener<RespT> {
      private final ServerCall<?, RespT> serverCall;
      // Hold 'this' lock when accessing
      private boolean needToRequest;

      public ResponseProxy(ServerCall<?, RespT> serverCall) {
        this.serverCall = serverCall;
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        serverCall.close(status, trailers);
      }

      @Override
      public void onHeaders(Metadata headers) {
        serverCall.sendHeaders(headers);
      }

      @Override
      public void onMessage(RespT message) {
        serverCall.sendMessage(message);
        synchronized (this) {
          if (serverCall.isReady()) {
            serverCallListener.clientCall.request(1);
          } else {
            // The incoming call is not ready for more responses. Stop requesting additional data
            // and wait for it to catch up.
            needToRequest = true;
          }
        }
      }

      @Override
      public void onReady() {
        serverCallListener.onClientReady();
      }

      // Called from RequestProxy, which is a different thread than the ClientCall.Listener
      // callbacks.
      synchronized void onServerReady() {
        if (needToRequest) {
          serverCallListener.clientCall.request(1);
          needToRequest = false;
        }
      }
    }
  }
}
