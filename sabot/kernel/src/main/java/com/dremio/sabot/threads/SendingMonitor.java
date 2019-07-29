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
package com.dremio.sabot.threads;

import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;

/**
 * Monitors the current outstanding message queue to ensure that we apply
 * pressure on the sending side of a connection when a receiving side is no
 * longer accepting data. Works with SharedResourceManager to inform when
 * execution should be blocked.
 */
public class SendingMonitor {

  @VisibleForTesting
  public static final int LIMIT = 3;

  private static final int RESTART = LIMIT - 1;
  private final AtomicInteger outsandingMessages = new AtomicInteger(0);
  private final SharedResource resource;
  private final SendingAccountor accountor;

  public SendingMonitor(SharedResource resource, SendingAccountor accountor) {
    super();
    this.resource = resource;
    this.accountor = accountor;
    resource.markAvailable();
  }

  public void increment(){
    accountor.increment();
    synchronized(resource){
      final int outcome = outsandingMessages.incrementAndGet();
      if (outcome == LIMIT) {
        resource.markBlocked();
      }
    }
  }

  private void decrement() {
    accountor.decrement();
    synchronized(resource) {
      final int outcome = outsandingMessages.decrementAndGet();
      if(outcome == RESTART){
        resource.markAvailable();
      }
    }
  }

  public RpcOutcomeListener<Ack> wrap(RpcOutcomeListener<Ack> listener){
    return new WrappedListener(listener);
  }

  private class WrappedListener implements RpcOutcomeListener<Ack>{
    private final RpcOutcomeListener<Ack> inner;

    public WrappedListener(RpcOutcomeListener<Ack> inner) {
      super();
      this.inner = inner;
    }

    @Override
    public void failed(RpcException ex) {
      inner.failed(ex);
      decrement();
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      inner.success(value, buffer);
      decrement();
    }

    @Override
    public void interrupted(InterruptedException e) {
      inner.interrupted(e);
      decrement();
    }
  }

}
