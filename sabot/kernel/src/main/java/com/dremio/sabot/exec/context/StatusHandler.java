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
package com.dremio.sabot.exec.context;

import io.netty.buffer.ByteBuf;

import com.dremio.common.DeferredException;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;

/**
 * Listener that keeps track of the status of batches sent, and updates the SendingAccountor when status is received
 * for each batch
 */
public class StatusHandler implements RpcOutcomeListener<Ack> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusHandler.class);
  private final DeferredException exception;

  public StatusHandler(DeferredException exception) {
    this.exception = exception;
  }

  @Override
  public void failed(RpcException ex) {
    exception.addException(ex);
  }

  @Override
  public void success(Ack value, ByteBuf buffer) {
    if (value.getOk()) {
      return;
    }

    logger.error("Data not accepted downstream. Stopping future sends.");
    // if we didn't get ack ok, we'll need to kill the query.
    exception.addException(new RpcException("Data not accepted downstream."));
  }

  @Override
  public void interrupted(final InterruptedException e) {
    exception.addException(e);
  }
}
