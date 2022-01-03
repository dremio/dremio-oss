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
package com.dremio.exec.rpc;

import io.netty.channel.ChannelFutureListener;

public interface ChannelListenerWithCoordinationId extends ChannelFutureListener {
  /**
   * Uniquely identifies a channel operation (e.g request id) so that it can be correlated with other related
   * operations (e.g response for a given request).
   *
   * @return an integer that uniquely identifies this operation.
   */
  int getCoordinationId();

  /**
   * Handles cases where the listener has started, but the channel operation did not start and the listener
   * was not yet attached to the channel operation.
   */
  void opNotStarted();
}
