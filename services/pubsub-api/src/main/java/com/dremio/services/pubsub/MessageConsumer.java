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

/** The interface implemented by subscription clients. */
public interface MessageConsumer<M extends Message> {
  /**
   * The implementation of the method should either ack (i.e. "done, ok to delete the message") or
   * nack (i.e. "please extend expiration and redeliver the message") the message after processing.
   * Failure to do so, may result in unexpected behavior: it is pubsub implementation dependent, in
   * general the message can be expected to be redelivered without extended expiration.
   */
  void process(MessageContainerBase<M> message);
}
