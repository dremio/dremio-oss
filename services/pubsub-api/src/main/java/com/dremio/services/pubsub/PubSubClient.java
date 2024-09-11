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

/** Factory class for instantiating publishers and subscribers. */
public interface PubSubClient {
  /**
   * Creates publisher for the given topic.
   *
   * @param topicClass Topic class provides name of the topic. The topic must be registered at
   *     infrastructure level before the publisher can be created.
   * @param options Publisher options including parameters for batching messages on the client.
   * @return {@link MessagePublisher} must be closed to free resources.
   */
  <M extends Message> MessagePublisher<M> getPublisher(
      Class<? extends Topic<M>> topicClass, MessagePublisherOptions options);

  /**
   * Creates subscriber for the given subscription.
   *
   * @param subscriptionClass Subscription class that provides subscription name and proto message
   *     parser. There could be multiple subscriptions per topic so names must be chosen with that
   *     in mind.
   * @param messageConsumer Callback to receive and acknowledge messages.
   * @param options Subscriber options.
   * @return {@link MessageSubscriber} that must be closed to terminate listening threads.
   */
  <M extends Message> MessageSubscriber<M> getSubscriber(
      Class<? extends Subscription<M>> subscriptionClass,
      MessageConsumer<M> messageConsumer,
      MessageSubscriberOptions options);
}
