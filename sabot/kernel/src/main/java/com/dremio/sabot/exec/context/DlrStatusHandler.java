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
package com.dremio.sabot.exec.context;

import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

public class DlrStatusHandler implements RpcOutcomeListener<GeneralRPCProtos.Ack> {

  StreamObserver streamObserver;

  public DlrStatusHandler(StreamObserver streamObserver) {
    this.streamObserver = streamObserver;
  }

  @Override
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  public void failed(RpcException ex) {
    streamObserver.onError(ex);
  }

  @Override
  public void success(GeneralRPCProtos.Ack value, ByteBuf buffer) {
    streamObserver.onCompleted();
  }

  @Override
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  public void interrupted(final InterruptedException e) {
    streamObserver.onError(e);
  }
}
