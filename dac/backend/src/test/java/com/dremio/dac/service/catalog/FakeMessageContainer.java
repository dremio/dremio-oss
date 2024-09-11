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
package com.dremio.dac.service.catalog;

import com.dremio.context.RequestContext;
import com.dremio.services.pubsub.MessageAckStatus;
import com.dremio.services.pubsub.MessageContainerBase;
import com.google.protobuf.Message;
import java.util.concurrent.CompletableFuture;

/** Test implementation of pubsub message. */
public final class FakeMessageContainer<M extends Message> extends MessageContainerBase<M> {
  private int ackCalls;
  private int nackCalls;

  public FakeMessageContainer(String id, M message, RequestContext requestContext) {
    super(id, message, requestContext);
  }

  public int getAckCalls() {
    return ackCalls;
  }

  public int getNackCalls() {
    return nackCalls;
  }

  @Override
  public CompletableFuture<MessageAckStatus> ack() {
    ackCalls++;
    return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
  }

  @Override
  public CompletableFuture<MessageAckStatus> nack() {
    nackCalls++;
    return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
  }
}
