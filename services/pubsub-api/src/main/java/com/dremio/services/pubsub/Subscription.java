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
import com.google.protobuf.Parser;

/**
 * After registering subscription at infrastructure layer, they must be registered in code by
 * implementing this interface.
 */
public interface Subscription<M extends Message> {
  /** Name of the subscription as defined by the pubsub implementation. */
  String getName();

  /** Proto parser for messages in this subscription, it must match the topic. */
  Parser<M> getMessageParser();

  /** Topic class. */
  Class<? extends Topic<M>> getTopicClass();
}
