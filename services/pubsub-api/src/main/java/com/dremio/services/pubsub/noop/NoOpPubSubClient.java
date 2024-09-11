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
package com.dremio.services.pubsub.noop;

import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessagePublisherOptions;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.MessageSubscriberOptions;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.google.protobuf.Message;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public final class NoOpPubSubClient implements PubSubClient {

  @Inject
  public NoOpPubSubClient() {}

  @Override
  public <M extends Message> MessagePublisher<M> getPublisher(
      Class<? extends Topic<M>> topicClass, MessagePublisherOptions options) {
    return new NoOpMessagePublisher<>();
  }

  @Override
  public <M extends Message> MessageSubscriber<M> getSubscriber(
      Class<? extends Subscription<M>> subscriptionClass,
      MessageConsumer<M> messageConsumer,
      MessageSubscriberOptions options) {
    return new NoOpMessageSubscriber<>();
  }
}
