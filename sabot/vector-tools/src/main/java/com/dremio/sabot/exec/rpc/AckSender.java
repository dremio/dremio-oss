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
package com.dremio.sabot.exec.rpc;

import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.exec.proto.ExecRPC.RpcType;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class that allows a group of receivers to confirm reception of a record batch as a single unit. Response
 * isn't send upstream until all receivers have successfully consumed data.
 */
public class AckSender {

  private AtomicInteger failed = new AtomicInteger(0);
  private AtomicInteger count = new AtomicInteger(0);
  private ResponseSender sender;

  @VisibleForTesting
  public AckSender(ResponseSender sender) {
    this.sender = sender;
  }

  /**
   * Add another sender to wait for.
   */
  void increment() {
    count.incrementAndGet();
  }

  /**
   * Disable any sending of the ok message.
   */
  void clear() {
    count.set(-100000);
  }

  /**
   * Decrement the number of references still holding on to this response. When the number of references hit zero, send
   * response upstream.
   */
  public void sendOk() {
    dec();
  }

  public void sendFail() {
    failed.incrementAndGet();
    dec();
  }

  public static final Response OK = new Response(RpcType.ACK, Acks.OK);
  public static final Response FAIL = new Response(RpcType.ACK, Acks.FAIL);


  private void dec(){
    if (0 == count.decrementAndGet()) {
      if(failed.get() == 0){
        sender.send(OK);
      } else {
        sender.send(FAIL);
      }
    }
  }
}
