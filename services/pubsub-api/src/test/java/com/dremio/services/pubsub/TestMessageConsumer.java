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
package com.dremio.services.pubsub;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class TestMessageConsumer<T extends Message> implements MessageConsumer<T> {
  private long messageProcessingDelayMillis;
  private volatile CountDownLatch latch;
  private final List<MessageContainerBase<T>> messages = new ArrayList<>();

  @Override
  public void process(MessageContainerBase<T> message) {
    if (messageProcessingDelayMillis > 0) {
      try {
        Thread.sleep(messageProcessingDelayMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    messages.add(message);
    latch.countDown();
  }

  public void setProcessingDelayMillis(long millis) {
    this.messageProcessingDelayMillis = millis;
  }

  public CountDownLatch initLatch(int count) {
    latch = new CountDownLatch(count);
    return latch;
  }

  public List<MessageContainerBase<T>> getMessages() {
    return messages;
  }
}
