/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.dremio.common.DeferredException;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;

import io.netty.buffer.ByteBuf;

/**
 * Allows a set of rpc Ack responses to be collected and considered upon completion.
 */
class CollectingOutcomeListener implements RpcOutcomeListener<Ack> {

  // start with one to make sure to not prematurely fire count down latch.
  private final AtomicLong counter = new AtomicLong(1);
  private final CountDownLatch latch = new CountDownLatch(1);

  private final DeferredException ex = new DeferredException();

  public void increment() {
    counter.incrementAndGet();
  }

  public void waitForFinish() throws Exception {
    // remove the extra one to avoid premature count down.
    decrement();
    latch.await();
    ex.close();
  }

  private void decrement() {
    long value = counter.decrementAndGet();
    if (value == 0) {
      latch.countDown();
    }
  }

  @Override
  public void failed(RpcException ex) {
    decrement();
    ex.addSuppressed(ex);
  }

  @Override
  public void success(Ack value, ByteBuf buffer) {
    decrement();
  }

  @Override
  public void interrupted(InterruptedException e) {
    decrement();
  }

}
